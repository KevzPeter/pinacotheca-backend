const { LexRuntimeV2Client, RecognizeTextCommand } = require("@aws-sdk/client-lex-runtime-v2");
const https = require("https");
const { URL } = require("url");

const OPENSEARCH_ENDPOINT = process.env.OPENSEARCH_ENDPOINT;
const OPENSEARCH_INDEX = process.env.OPENSEARCH_INDEX || "photos";
const OPENSEARCH_USERNAME = process.env.OPENSEARCH_USERNAME;
const OPENSEARCH_PASSWORD = process.env.OPENSEARCH_PASSWORD;

const LEX_BOT_ID = process.env.LEX_BOT_ID;
const LEX_BOT_ALIAS_ID = process.env.LEX_BOT_ALIAS_ID;
const LEX_LOCALE_ID = process.env.LEX_LOCALE_ID || "en_US";

// v3 Lex client – uses Lambda's IAM role + region by default
const lexClient = new LexRuntimeV2Client({});

// Call Lex RecognizeText and extract keywords from the Keywords slot
async function getKeywordsFromLex(queryText) {
    if (!LEX_BOT_ID || !LEX_BOT_ALIAS_ID) {
        throw new Error("Lex bot environment variables not set");
    }

    const params = {
        botId: LEX_BOT_ID,
        botAliasId: LEX_BOT_ALIAS_ID,
        localeId: LEX_LOCALE_ID,
        sessionId: "photos-session", // can be any identifier
        text: queryText
    };

    const resp = await lexClient.send(new RecognizeTextCommand(params));
    console.log(`INTENT NAME ====> ${resp.sessionState.intent.name}`);
    const intent = resp.sessionState && resp.sessionState.intent;
    if (!intent || intent.name !== "SearchIntent") {
        return [];
    }

    const slots = intent.slots || {};
    const keywordSlot = slots.keyword;
    if (!keywordSlot || !keywordSlot.value || !keywordSlot.value.interpretedValue) {
        return [];
    }

    const raw = keywordSlot.value.interpretedValue.toLowerCase();

    // Very simple keyword extraction:
    // - remove "and"
    // - split on spaces/commas
    // - trim and deduplicate
    const parts = raw
        .replace(/\band\b/g, " ")
        .split(/[,\s]+/)
        .map(s => s.trim())
        .filter(s => s.length > 0);

    return Array.from(new Set(parts));
}

// Query OpenSearch for any of the keywords in the labels field
function searchOpenSearch(keywords) {
    return new Promise((resolve, reject) => {
        if (!OPENSEARCH_ENDPOINT) {
            return reject(new Error("OPENSEARCH_ENDPOINT not set"));
        }
        if (!keywords || keywords.length === 0) {
            return resolve([]);
        }

        const endpoint = new URL(OPENSEARCH_ENDPOINT);
        const path = `/${OPENSEARCH_INDEX}/_search`;

        const body = JSON.stringify({
            query: {
                terms: {
                    labels: keywords
                }
            }
        });

        const auth = Buffer.from(
            `${OPENSEARCH_USERNAME}:${OPENSEARCH_PASSWORD}`
        ).toString("base64");

        const options = {
            hostname: endpoint.hostname,
            port: endpoint.port || 443,
            path,
            method: "POST",
            protocol: "https:",
            headers: {
                "Content-Type": "application/json",
                "Content-Length": Buffer.byteLength(body),
                "Authorization": `Basic ${auth}`
            }
        };

        const req = https.request(options, res => {
            let data = "";
            res.on("data", chunk => (data += chunk));
            res.on("end", () => {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    try {
                        const json = JSON.parse(data);
                        const hits = (json.hits && json.hits.hits) || [];
                        resolve(hits);
                    } catch (err) {
                        reject(err);
                    }
                } else {
                    console.error("OpenSearch search error:", res.statusCode, data);
                    reject(
                        new Error(
                            `OpenSearch search failed with status ${res.statusCode}`
                        )
                    );
                }
            });
        });

        req.on("error", err => {
            console.error("HTTPS search error:", err);
            reject(err);
        });

        req.write(body);
        req.end();
    });
}

exports.handler = async (event) => {
    try {
        console.log("Incoming event:", JSON.stringify(event));

        // For API Gateway proxy: /search?q=...
        const q =
            (event.queryStringParameters && event.queryStringParameters.q) ||
            event.q ||
            "";

        if (!q || q.trim().length === 0) {
            return {
                statusCode: 200,
                headers: {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,x-api-key",
                    "Access-Control-Allow-Methods": "GET,OPTIONS"
                },
                body: JSON.stringify([])
            };
        }

        const queryText = q.trim();
        console.log("Search query:", queryText);

        // 1. Get keywords from Lex
        const keywords = await getKeywordsFromLex(queryText);
        console.log("Extracted keywords:", keywords);

        if (!keywords || keywords.length === 0) {
            // Assignment requirement: empty array if no keywords
            return {
                statusCode: 200,
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify([])
            };
        }

        // 2. Search OpenSearch
        const hits = await searchOpenSearch(keywords);

        // 3. Map hits to simple JSON objects
        const results = hits.map(h => {
            const src = h._source || {};
            const bucket = src.bucket;
            const key = src.objectKey;

            return {
                objectKey: key,
                bucket,
                labels: src.labels,
                url: bucket && key
                    ? `https://${bucket}.s3.amazonaws.com/${encodeURIComponent(key)}`
                    : null
            };
        });
        return {
            statusCode: 200,
            headers: {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,x-api-key",
                "Access-Control-Allow-Methods": "GET,OPTIONS"
            },
            body: JSON.stringify(results)
        };
    } catch (err) {
        console.error("Error in search-photos:", err);

        return {
            statusCode: 500,
            headers: {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,x-api-key",
                "Access-Control-Allow-Methods": "GET,OPTIONS"
            },
            body: JSON.stringify({
                message: "Internal server error",
                error: err.message
            })
        };
    }
};
