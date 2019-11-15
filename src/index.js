const MongoClient = require('mongodb').MongoClient;
const uri = "mongodb://172.20.0.2:27017/test?retryWrites=true&w=majority";
const client = new MongoClient(uri, {useNewUrlParser: true, useUnifiedTopology: true});
const fs = require('fs');
const dbName = 'myproject';

function groupBy(list, keyGetter) {
    const map = new Map();
    list.forEach((item) => {
        const key = keyGetter(item);
        const collection = map.get(key);
        if (!collection) {
            map.set(key, [item]);
        } else {
            collection.push(item);
        }
    });
    return map;
}

client.connect(async err => {
    if (process.env.INSERT_DATA === 'true') {
        const d = fs.readFileSync('/home/omkar/Downloads/2019-01-01-15.json').toString()
            .split('\n')
            .filter(res => !!res.trim())
            .map(res => JSON.parse(res))
            .map(res => res);

        const map = groupBy(d, d => d.type);

        map.forEach(async (e, data) => {
            console.log(data);
            try {
                await client.db(dbName)
                    .collection('data')
                    .insertMany(e);
            } catch (e) {
                console.log(data, 'failed');
            }
            console.log(data, 'done');
        });
    }
});

const express = require('express');
const app = express();
app.get('/api1', async (res, req) => {
    console.log({repo: {id: parseInt(res.query.id)}, type: res.query.type});
    await client.db(dbName)
        .collection('data')
        .find({'repo.id': parseInt(res.query.id), type: res.query.type})
        .toArray(function (err, docs) {
            if (err) {
                return req.status(500).send(err);
            }
            console.log(docs);
            req.send(docs);
        });
});

app.get('/api2', (res, req) => {
    client.db(dbName)
        .collection('data')
        .find({'actor.login': res.query.user_id})
        .toArray((err, docs) => {
            if (err) {
                return req.status(500).send(err);
            }
            req.send(docs);
        })
});

app.get('/api3', (res, req) => {
    client.db(dbName)
        .collection('data')
        .aggregate([{$match: {"actor.login": req.query.actor}}, {$project: {_id: "$repo.id"}}, {
            $group: {
                _id: "$_id",
                "count": {$sum: 1}
            }
        }, {$sort: {count: 1}}], {allowDiskUse: true})
        .toArray((err, docs) => {
            if (err) {
                return req.status(500).send(err);
            }
            if (docs.length === 0) {
                return req.sendStatus(404);
            }
            req.send(docs[0]);
        })
});

app.get('/api4', (res, req) => {
    client.db(dbName)
        .collection('data')
        .aggregate([{$project: {_id: "$repo.id", user_id: "$actor.id"}}, {
            $group: {
                _id: "$_id",
                "contributers": {$addToSet: {user_id: "$user_id", count: {$sum: 1}}}
            },
        }], {allowDiskUse: true})
        .toArray((err, docs) => {
            console.log('called');
            if (err) {
                return req.status(500).send(err);
            }
            const returnValue = docs.map(res => {
                res.contributers = res.contributers[0];
                return res;
            })
            req.send(returnValue);
        })
});

app.delete('/api5', (res,req) => {
    client.db(dbName)
        .collection('data')
        .deleteMany({'actor.login':res.query.id})
        .then(res1 => {
            req.send(res1.deletedCount)
        })

})

app.listen(3000);
