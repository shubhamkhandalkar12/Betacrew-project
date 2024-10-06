// betacrew_client.js
const net = require('net');
const fs = require('fs');

// Define server details
const HOST = '127.0.0.1'; // Change to the server's IP if needed
const PORT = 3000;

// Function to create request payload
function createRequestPayload(callType, resendSeq = null) {
    const buffer = Buffer.alloc(6);
    buffer.writeUInt8(callType, 0);
    if (callType === 2 && resendSeq !== null) {
        buffer.writeUInt8(resendSeq, 1);
    }
    return buffer;
}

// Function to parse the response packet
function parsePacket(data) {
    const symbol = data.slice(0, 4).toString('ascii').trim();
    const buySellIndicator = data.readUInt8(4);
    const quantity = data.readUInt32BE(5);
    const price = data.readUInt32BE(9);
    const sequence = data.readUInt32BE(13);
    
    return {
        symbol: symbol,
        buySell: String.fromCharCode(buySellIndicator),
        quantity: quantity,
        price: price,
        sequence: sequence
    };
}

// Main function to connect to the server and fetch data
async function fetchData() {
    return new Promise((resolve, reject) => {
        const client = net.createConnection({ host: HOST, port: PORT }, () => {
            console.log('Connected to server');
            // Send request for streaming all packets
            const requestPayload = createRequestPayload(1);
            client.write(requestPayload);
        });

        let packets = [];
        client.on('data', (data) => {
            let offset = 0;
            while (offset < data.length) {
                const packet = data.slice(offset, offset + 17); // Each packet is 17 bytes
                packets.push(parsePacket(packet));
                offset += 17;
            }
        });

        client.on('end', () => {
            console.log('Disconnected from server');
            resolve(packets);
        });

        client.on('error', (err) => {
            reject(err);
        });
    });
}

// Function to handle missing sequences
async function handleMissingSequences(packets) {
    const receivedSequences = new Set(packets.map(p => p.sequence));
    const totalPackets = packets.length;

    for (let seq = 0; seq < totalPackets; seq++) {
        if (!receivedSequences.has(seq)) {
            const client = net.createConnection({ host: HOST, port: PORT }, () => {
                const requestPayload = createRequestPayload(2, seq);
                client.write(requestPayload);
            });

            client.on('data', (data) => {
                const packet = parsePacket(data);
                packets.push(packet);
                client.end();
            });

            client.on('end', () => {
                console.log(`Resent packet sequence ${seq}`);
            });
        }
    }
}

// Function to write data to JSON file
function writeToJsonFile(data) {
    fs.writeFileSync('ticker_data.json', JSON.stringify(data, null, 2));
    console.log('Data written to ticker_data.json');
}

// Main execution
(async () => {
    try {
        const packets = await fetchData();
        await handleMissingSequences(packets);
        writeToJsonFile(packets);
    } catch (error) {
        console.error('Error:', error);
    }
})();