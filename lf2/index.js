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

const lexClient = new LexRuntimeV2Client({});

//  helper for CORS responses
function makeResponse(statusCode, bodyObj) {
    return {
        statusCode,
        headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,x-api-key",
            "Access-Control-Allow-Methods": "GET,OPTIONS"
        },
        body: JSON.stringify(bodyObj)
    };
}

// expand simple singular / plural variants for better matching
function expandSingularPlural(words) {
    const out = new Set();

    for (const wRaw of words || []) {
        if (!wRaw) continue;
        const w = wRaw.toLowerCase().trim();
        if (!w) continue;
        out.add(w);

        // plural -> singular
        if (w.endsWith("ies") && w.length > 3) {
            // babies -> baby
            out.add(w.slice(0, -3) + "y");
        } else if (w.endsWith("es") && w.length > 3) {
            // buses -> bus (naive)
            out.add(w.slice(0, -2));
        } else if (w.endsWith("s") && w.length > 3) {
            // cats -> cat
            out.add(w.slice(0, -1));
        }

        // singular -> plural
        if (!w.endsWith("s")) {
            if (w.endsWith("y") && w.length > 1) {
                // baby -> babies
                out.add(w.slice(0, -1) + "ies");
            }
            // cat -> cats
            out.add(w + "s");
        }
    }

    return Array.from(out);
}

// normalize a raw slot string into tokens
function tokenizeSlotValue(raw) {
    if (!raw) return [];
    const lowered = raw.toLowerCase();

    return Array.from(
        new Set(
            lowered
                .replace(/\band\b/g, " ")
                .split(/[,\s]+/)
                .map(s => s.trim())
                .filter(s => s.length > 0)
        )
    );
}

// call Lex RecognizeText and extract keywords from keyword1 / keyword2 slots
async function getKeywordsFromLex(queryText) {
    if (!LEX_BOT_ID || !LEX_BOT_ALIAS_ID) {
        throw new Error("Lex bot environment variables not set");
    }

    const params = {
        botId: LEX_BOT_ID,
        botAliasId: LEX_BOT_ALIAS_ID,
        localeId: LEX_LOCALE_ID,
        sessionId: "photos-session",
        text: queryText
    };

    const resp = await lexClient.send(new RecognizeTextCommand(params));
    console.log("Lex full response:", JSON.stringify(resp));
    const intent = resp.sessionState && resp.sessionState.intent;
    console.log(`INTENT NAME ====> ${intent && intent.name}`);

    if (!intent || intent.name !== "SearchIntent") {
        return [];
    }

    const slots = intent.slots || {};
    const collected = [];
    const slotNames = ["keyword1", "keyword2"];

    for (const name of slotNames) {
        const slot = slots[name];
        if (!slot || !slot.value) continue;

        const v =
            slot.value.interpretedValue ||
            (Array.isArray(slot.value.resolvedValues) && slot.value.resolvedValues[0]) ||
            slot.value.originalValue;

        if (v && v.trim().length > 0) {
            const tokens = tokenizeSlotValue(v);
            collected.push(...tokens);
        }
    }

    if (collected.length === 0 && slots.keyword && slots.keyword.value) {
        const v =
            slots.keyword.value.interpretedValue ||
            (Array.isArray(slots.keyword.value.resolvedValues) &&
                slots.keyword.value.resolvedValues[0]) ||
            slots.keyword.value.originalValue;

        if (v && v.trim().length > 0) {
            const tokens = tokenizeSlotValue(v);
            collected.push(...tokens);
        }
    }

    const unique = Array.from(new Set(collected));
    console.log("Lex raw keywords (unique):", unique);
    return unique;
}

// query OpenSearch for any of the keywords in the labels field
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
                bool: {
                    should: [
                        {
                            terms: {
                                labels: keywords
                            }
                        }
                    ],
                    minimum_should_match: 1
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

        const q =
            (event.queryStringParameters && event.queryStringParameters.q) ||
            event.q ||
            "";

        if (!q || q.trim().length === 0) {
            return makeResponse(200, []);
        }

        const queryText = q.trim();
        console.log("Search query:", queryText);

        // get raw keywords from Lex (keyword1 / keyword2)
        const rawKeywords = await getKeywordsFromLex(queryText);
        console.log("Extracted keywords from Lex:", rawKeywords);

        // expand to include simple singular / plural variants
        const searchTerms = expandSingularPlural(rawKeywords);
        console.log("Search terms after singular/plural expansion:", searchTerms);

        if (!searchTerms || searchTerms.length === 0) {
            return makeResponse(200, []);
        }

        const hits = await searchOpenSearch(searchTerms);

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

        return makeResponse(200, results);
    } catch (err) {
        console.error("Error in search-photos:", err);

        return makeResponse(500, {
            message: "Internal server error",
            error: err.message
        });
    }
};
