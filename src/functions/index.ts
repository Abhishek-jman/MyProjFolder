import { app, HttpRequest, HttpResponseInit, InvocationContext } from "@azure/functions";
import { EventHubProducerClient, EventDataBatch } from "@azure/event-hubs";
import { BlobServiceClient } from '@azure/storage-blob';

// Get environment variables for connection strings
const eventHubConnectionStr = process.env["EventHubConnectionString"];
const azureBlobStorageConnectionString = process.env["AzureWebJobsStorage"];

// Ensure the connection strings are available
if (!eventHubConnectionStr || !azureBlobStorageConnectionString) {
    throw new Error("Connection strings for Event Hub or Blob Storage are not set.");
}

// Initialize Blob Service Client
const blobServiceClient = BlobServiceClient.fromConnectionString(azureBlobStorageConnectionString);
const containerClient = blobServiceClient.getContainerClient('event-data');

// Define the HubSpotWebhook function
export async function HubSpotWebhook(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
    context.log('HubSpot webhook received.');

    let requestBody;
    try {
        requestBody = await request.json();
    } catch (error) {
        return {
            status: 400,
            body: "Invalid JSON"
        };
    }

    const eventHubName = "hubspot_1";
    const producer = new EventHubProducerClient(eventHubConnectionStr, eventHubName);

    let eventDataBatch: EventDataBatch;
    try {
        eventDataBatch = await producer.createBatch();
        eventDataBatch.tryAdd({ body: JSON.stringify(requestBody) });

        await producer.sendBatch(eventDataBatch);
        context.log("Event sent to Event Hub");
    } catch (error) {
        context.log(`Error sending event: ${error}`);
        return {
            status: 500,
            body: "Error sending event to Event Hub"
        };
    } finally {
        await producer.close();
    }

    return {
        status: 200,
        body: "Event received"
    };
}

app.http('HubSpotWebhook', {
    methods: ['POST'],
    authLevel: 'anonymous',
    handler: HubSpotWebhook
});

// Define the EventHubTrigger function
export async function eventHubTrigger(messages: unknown, context: InvocationContext): Promise<void> {
    context.log('Event hub function processed message:', messages);

    // Ensure the container exists
    await containerClient.createIfNotExists();

    // Handle single or batched messages based on cardinality
    const events = Array.isArray(messages) ? messages : [messages];

    // API

    for (const message of events) {
        const blobName = `event-${new Date().toISOString()}.json`;
        const blockBlobClient = containerClient.getBlockBlobClient(blobName);
        await blockBlobClient.upload(JSON.stringify(message), Buffer.byteLength(JSON.stringify(message)));
        context.log(`Stored event in blob: ${blobName}`);
    }

    // Log metadata
    context.log('EnqueuedTimeUtc =', context.triggerMetadata.enqueuedTimeUtc);
    context.log('SequenceNumber =', context.triggerMetadata.sequenceNumber);
    context.log('Offset =', context.triggerMetadata.offset);
}

app.eventHub('PersistEventsFunction', {
    connection: 'EventHubConnectionString',
    eventHubName: 'hubspot_1',
    cardinality: 'many', // Adjust to 'one' if you are handling single messages
    handler: eventHubTrigger,
});
