const { S3Client, HeadObjectCommand } = require("@aws-sdk/client-s3");
const { RekognitionClient, DetectLabelsCommand } = require("@aws-sdk/client-rekognition");
const https = require("https");
const { URL } = require("url");

// v3 clients – no config needed if using Lambda's IAM role/region defaults
const s3 = new S3Client({});
const rekognition = new RekognitionClient({});

const OPENSEARCH_ENDPOINT = process.env.OPENSEARCH_ENDPOINT;
const OPENSEARCH_INDEX = process.env.OPENSEARCH_INDEX || 'photos';
const OPENSEARCH_USERNAME = process.env.OPENSEARCH_USERNAME;
const OPENSEARCH_PASSWORD = process.env.OPENSEARCH_PASSWORD;

// Helper: HTTP POST with Basic Auth to OpenSearch
function indexDocumentToOpenSearch(doc) {
    return new Promise((resolve, reject) => {
        if (!OPENSEARCH_ENDPOINT) {
            return reject(new Error('OPENSEARCH_ENDPOINT not set'));
        }

        const endpoint = new URL(OPENSEARCH_ENDPOINT);
        const path = `/${OPENSEARCH_INDEX}/_doc`;

        const body = JSON.stringify(doc);
        const auth = Buffer.from(
            `${OPENSEARCH_USERNAME}:${OPENSEARCH_PASSWORD}`
        ).toString('base64');

        const options = {
            hostname: endpoint.hostname,
            port: endpoint.port || 443,
            path,
            method: 'POST',
            protocol: 'https:',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(body),
                'Authorization': `Basic ${auth}`
            }
        };

        const req = https.request(options, res => {
            let data = '';
            res.on('data', chunk => (data += chunk));
            res.on('end', () => {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    resolve({ statusCode: res.statusCode, body: data });
                } else {
                    console.error('OpenSearch error:', res.statusCode, data);
                    reject(
                        new Error(
                            `OpenSearch indexing failed with status ${res.statusCode}`
                        )
                    );
                }
            });
        });

        req.on('error', err => {
            console.error('HTTPS request error:', err);
            reject(err);
        });

        req.write(body);
        req.end();
    });
}

// Helper: parse custom labels from S3 object metadata
function extractCustomLabelsFromMetadata(metadata) {
    if (!metadata) return [];

    // S3 lowercases user-defined metadata keys
    const raw = metadata['customlabels'] || metadata['custom-labels'];
    if (!raw) return [];

    return raw
        .split(',')
        .map(s => s.trim())
        .filter(s => s.length > 0);
}

exports.handler = async (event) => {
    console.log('Received S3 event:', JSON.stringify(event, null, 2));

    // Handle potentially multiple records in one event
    const records = event.Records || [];

    const results = [];

    for (const record of records) {
        const bucket = record.s3.bucket.name;
        const key = decodeURIComponent(
            record.s3.object.key.replace(/\+/g, ' ')
        );
        const eventTime = record.eventTime || new Date().toISOString();

        console.log(`Processing object: s3://${bucket}/${key}`);

        try {
            // 1. Detect labels with Rekognition
            const detectParams = {
                Image: {
                    S3Object: {
                        Bucket: bucket,
                        Name: key
                    }
                },
                MaxLabels: 10,
                MinConfidence: 80
            };

            const detectResp = await rekognition.send(
                new DetectLabelsCommand(detectParams)
            );

            const rekognitionLabels =
                (detectResp.Labels || [])
                    .map(l => l.Name ? l.Name.toLowerCase() : null)
                    .filter(Boolean);

            console.log('Rekognition labels:', rekognitionLabels);

            // 2. Read S3 object metadata for custom labels
            const headResp = await s3.send(
                new HeadObjectCommand({ Bucket: bucket, Key: key })
            );

            const customLabelsRaw = extractCustomLabelsFromMetadata(headResp.Metadata);
            const customLabels = customLabelsRaw
                .map(s => s.toLowerCase())
                .filter(Boolean);

            console.log('Custom labels:', customLabels);

            // 3. Merge and de-duplicate labels
            const mergedLabels = Array.from(
                new Set([
                    ...rekognitionLabels,
                    ...customLabels
                ])
            );

            // 4. Build document to index
            const doc = {
                objectKey: key,
                bucket,
                createdTimestamp: eventTime,
                labels: mergedLabels
            };

            console.log('Indexing document:', JSON.stringify(doc));

            // 5. Index into OpenSearch
            const resp = await indexDocumentToOpenSearch(doc);
            console.log('OpenSearch response:', resp);

            results.push({
                key,
                status: 'OK'
            });

        } catch (err) {
            if (err.Code === "InvalidImageFormatException" || err.name === "InvalidImageFormatException") {
                console.warn(`Skipping invalid image s3://${bucket}/${key}:`, err.message);
                results.push({ key, status: "SKIPPED_INVALID_IMAGE", error: err.message });
            } else {
                console.error(`Error processing s3://${bucket}/${key}:`, err);
                results.push({ key, status: "ERROR", error: err.message });
            }
        }
    }

    return {
        statusCode: 200,
        body: JSON.stringify(results)
    };
};
