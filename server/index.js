const express = require('express');
const { createServer } = require('node:http');
const { join } = require('node:path');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3');
const { open} = require('sqlite');
const { availableParallelism } = require('node:os');
const cluster = require('node:cluster');
const { createAdapter, setupPrimary } = require("@socket.io/cluster-adapter");

if (cluster.isPrimary) {
    const numCPUs = availableParallelism();

    // create one worker per available core
    for (let i = 0; i < numCPUs; i++) {
        cluster.fork({
            PORT: 3000 + i
        });
    }

    // set up the adapter on the primary thread
    return setupPrimary();
}


async function main() {
    // open db file
    const db = await open({
        filename: 'chat.db',
        driver: sqlite3.Database
    });

    // create messages table
    await db.exec(`
        CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_offset TEXT UNIQUE,
        content TEXT
        );
    `);

    const app = express();
    const server = createServer(app);
    const io = new Server(server, {
        connectionStateRecovery: {},
        // set up the adapter on each worker thread
        adapter: createAdapter()
    });
    
    app.get('/', (req, res) => {
        res.sendFile(join(__dirname, '../client/index.html'))
    });
    
    io.on('connection', async (socket) => {
        console.log("a user connected");
        socket.on('disconnect', () => {
            console.log('user disconnected');
        });
    
        socket.on('chat message', async (msg, clientOffset, callback) => {
            let result;
            try {
                result = await db.run('INSERT INTO messages (content, client_offset) VALUES (?, ?)', msg, clientOffset);
            } catch (error) {
                console.log(error);
                if (e.errno === 19) {
                    callback();
                } else {

                }
                return;
            }
            io.emit('chat message', msg, result.lastID);
            callback();
        });

        if (!socket.recovered) {
            try {
                await db.each('SELECT id, content FROM messages WHERE id > ?',
                    [socket.handshake.auth.serverOffset || 0],
                    (_err, row) => {
                        socket.emit('chat message', row.content, row.id);
                    }
                )
            } catch(e) {
                console.log(e);
            }
        }
    
    
    });
    
    const port = process.env.PORT;
    
    server.listen(port, () => {
        console.log('Server running at http://localhost:3000');
    });

}


main();