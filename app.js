"use strict"

const { Pool } = require('pg');
const fs = require('fs');
const dateFormat = require('dateformat');
const readline = require('readline').createInterface({
  input: process.stdin,
  output: process.stdout
});

// let p = (x)=>{console.log(x.match(/\S+/g).join(" ")); return x;};
let p = x => x;

async function *getCommandIterator(){
    for await (const command of readline) {
        yield command;
    }
    readline.close();
}

async function openDbAndGetPool(iterator) {

    let { open: { database, login, password } } =
        JSON.parse((await iterator.next()).value);
    return new Pool({
        user: login,
        host: 'localhost',
        database: database,
        password: password,
        port: 5432
    });
}

if (process.argv.length > 3 || (process.argv.length == 3
        && process.argv[2] != "--init")) {

    console.error(`The program can only be run with one argument, which\
is "--init", or without arguments, but you have given such arguments:\
"${ process.argv.slice(2).join(" ") }"`);
    readline.close();

} else if (process.argv.length == 3) {

    (async function() {

        let it = getCommandIterator();

        let pool_;
        let client_;
        try {
            pool_ = await openDbAndGetPool(it);
            client_ = await pool_.connect();

            console.log(JSON.stringify({ status: "OK" }));
        } catch (err) {
            console.log(JSON.stringify({ status: "ERROR" }));
            for await (const command of it) {
                console.log(JSON.stringify({ status: "ERROR" }));
            }
            // console.error("Could not connect to the database.");
            // console.error(err);
            try {
                client.release();
            } catch (err) {}
            try {
                pool.end();
            } catch (err) {}
            return;
        }

        const pool = pool_;
        const client = client_;

        let sql = fs.readFileSync('physical_model.sql').toString();
        try {
            await client.query(sql);
        } catch(err) {
            console.error("Executing sql file failed.");
            console.error(err);
            try {
                client.release();
            } catch (err) {}
            try {
                pool.end();
            } catch (err) {}
            return;
        }

        for await (const command of it) {
            let helpObject;
            try {
                helpObject = JSON.parse(command);
            } catch (err) {
                console.error(err);
                continue;
            }
            let { leader: { timestamp, password, member } } = helpObject;
            let res;
            try {
                await client.query(p(`SELECT
                    leader(
                        '${ dateFormat(
                            new Date(timestamp * 1000
                        ), "yyyy-mm-dd HH:MM:ss") }'::TIMESTAMP,
                        '${ password }'::TEXT,
                        ${ member }::NUMERIC
                    );`));
                res = {
                    status: "OK"
                };
            } catch (err) {
                res = {
                    status: "ERROR"
                };
                // console.error(err);
            }
            console.log(JSON.stringify(res));
        }

        try {
            client.release();
        } catch (err) {}
        try {
            pool.end();
        } catch (err) {}
        try {
            readline.close();
        } catch (err) {}
})();

} else {

    (async function() {
        let it = getCommandIterator();

        let pool_;
        let client_;
        try {
            pool_ = await openDbAndGetPool(it);
            client_ = await pool_.connect();

            console.log(JSON.stringify({ status: "OK" }));
        } catch (err) {
            console.log(JSON.stringify({ status: "ERROR" }));
            for await (const command of it) {
                console.log(JSON.stringify({ status: "ERROR" }));
            }
            // console.error("Could not connect to the database.");
            // console.error(err);
            try {
                client.release();
            } catch (err) {}
            try {
                pool.end();
            } catch (err) {}
            return;
        }
        const pool = pool_;
        const client = client_;

        for await (const command of it) {
            let helpObject;
            try {
                helpObject = JSON.parse(command);
            } catch (err) {
                console.error(err);
                continue;
            }

            switch (Object.keys(helpObject)[0]) {
                case 'support':
                    {
                        let { support: { timestamp, password, member, action,
                            project, authority } } = helpObject;
                        let res;
                        try {
                            await client.query(p(`SELECT
                                support(
                                    '${ dateFormat(
                                        new Date(timestamp * 1000
                                    ), "yyyy-mm-dd HH:MM:ss") }'::TIMESTAMP,
                                    ${ member }::NUMERIC,
                                    '${ password }'::TEXT,
                                    ${ action }::NUMERIC,
                                    ${ project }::NUMERIC
                                    ${ authority ? `, ${ authority }::NUMERIC` : "" }
                                );`));
                            res = {
                                status: "OK"
                            };
                        } catch (err) {
                            res = {
                                status: "ERROR"
                            };
                            // console.error(err);
                        }
                        console.log(JSON.stringify(res));
                    }
                    break;
                case 'protest':
                    {
                        let { protest: { timestamp: timestamp, password, member,
                            action, project, authority } } = helpObject;
                        let res;
                        try {
                            await client.query(p(`SELECT
                                protest(
                                    '${ dateFormat(
                                        new Date(timestamp * 1000
                                    ), "yyyy-mm-dd HH:MM:ss") }'::TIMESTAMP,
                                    ${ member }::NUMERIC,
                                    '${ password }'::TEXT,
                                    ${ action }::NUMERIC,
                                    ${ project }::NUMERIC
                                    ${ authority ? `, ${ authority }::NUMERIC` : ""  }
                                );`));
                            res = {
                                status: "OK"
                            };
                        } catch (err) {
                            res = {
                                status: "ERROR"
                            };
                            // console.error(err);
                        }
                        console.log(JSON.stringify(res));
                    }
                    break;
                case 'upvote':
                    {
                        let { upvote: { timestamp, password, member,
                            action } } = helpObject;
                        let res;
                        try {
                            await client.query(p(`SELECT
                                upvote(
                                    '${ dateFormat(
                                        new Date(timestamp * 1000
                                    ), "yyyy-mm-dd HH:MM:ss") }'::TIMESTAMP,
                                    ${ member }::NUMERIC,
                                    '${ password }'::TEXT,
                                    ${ action }::NUMERIC
                                );`));
                            res = {
                                status: "OK"
                            };
                        } catch (err) {
                            res = {
                                status: "ERROR"
                            };
                            // console.error(err);
                        }
                        console.log(JSON.stringify(res));
                    }
                    break;
                case 'downvote':
                    {
                        let { downvote: { timestamp, password, member,
                            action } } = helpObject;
                        let res;
                        try {
                            await client.query(p(`SELECT
                                downvote(
                                    '${ dateFormat(
                                        new Date(timestamp * 1000
                                    ), "yyyy-mm-dd HH:MM:ss") }'::TIMESTAMP,
                                    ${ member }::NUMERIC,
                                    '${ password }'::TEXT,
                                    ${ action }::NUMERIC
                                );`));
                            res = {
                                status: "OK"
                            };
                        } catch (err) {
                            res = {
                                status: "ERROR"
                            };
                            // console.error(err);
                        }
                        console.log(JSON.stringify(res));
                    }
                    break;
                case 'actions':
                    {
                        let { actions: { timestamp, password, member,
                            type, project, authority } } = helpObject;
                        let res;
                        try {
                            let { rows } = await client.query(p(`SELECT * FROM
                                get_actions(
                                    '${ dateFormat(
                                        new Date(timestamp * 1000
                                    ), "yyyy-mm-dd HH:MM:ss") }'::TIMESTAMP,
                                    ${ member }::NUMERIC,
                                    '${ password }'::TEXT
                                    ${ type ? `, '${ type }'` : ", null" }
                                    ${ project ? ", 'project'" :
                                        authority ? ", 'authority'" : "" }
                                    ${ project ? `, ${ project }::NUMERIC` :
                                        authority ?
                                        `, ${ authority }::NUMERIC` : "" }
                                );`));
                            rows = rows.map(row => [
                                    +row.action_id,
                                    row.type,
                                    +row.project_id,
                                    +row.authority_id,
                                    +row.upvotes,
                                    +row.downvotes
                                ]);
                            res = {
                                status: "OK",
                                data: rows
                            };
                        } catch (err) {
                            res = {
                                status: "ERROR"
                            };
                            // console.error(err);
                        }
                        console.log(JSON.stringify(res));
                    }
                    break;
                case 'projects':
                    {
                        let { projects: { timestamp, password, member,
                            authority } } = helpObject;
                        let res;
                        try {
                            let { rows } = await client.query(p(`SELECT * FROM
                                get_projects(
                                    '${ dateFormat(
                                        new Date(timestamp * 1000
                                    ), "yyyy-mm-dd HH:MM:ss") }'::TIMESTAMP,
                                    ${ member }::NUMERIC,
                                    '${ password }'::TEXT
                                    ${ authority ? `, ${ authority }::NUMERIC` : "" }
                                );`));
                            rows = rows.map(row => [
                                    +row.project_id,
                                    +row.authority_id
                                ]);
                            res = {
                                status: "OK",
                                data: rows
                            };
                        } catch (err) {
                            res = {
                                status: "ERROR"
                            };
                            // console.error(err);
                        }
                        console.log(JSON.stringify(res));
                    }
                    break;
                case 'votes':
                    {
                        let { votes: { timestamp, password, member,
                            action, project } } = helpObject;
                        let res;
                        try {
                            let { rows } = await client.query(p(`SELECT * FROM
                                get_votes(
                                    '${ dateFormat(
                                        new Date(timestamp * 1000
                                    ), "yyyy-mm-dd HH:MM:ss") }'::TIMESTAMP,
                                    ${ member }::NUMERIC,
                                    '${ password }'::TEXT
                                    ${ action ? ", 'action'" :
                                        project ? ", 'project'" : "" }
                                    ${ action ? `, ${ action }::NUMERIC` :
                                        project ? `, ${ project }::NUMERIC` : "" }
                                );`));
                            rows = rows.map(row => [
                                    +row.member_id,
                                    +row.upvotes,
                                    +row.downvotes
                                ]);
                            res = {
                                status: "OK",
                                data: rows
                            };
                        } catch (err) {
                            res = {
                                status: "ERROR"
                            };
                            // console.error(err);
                        }
                        console.log(JSON.stringify(res));
                    }
                    break;
                case 'trolls':
                    {
                        let { trolls: { timestamp } } = helpObject;
                        let res;
                        try {
                            let { rows } = await client.query(p(`SELECT * FROM
                                get_trolls(
                                    '${ dateFormat(
                                        new Date(timestamp * 1000
                                    ), "yyyy-mm-dd HH:MM:ss") }'::TIMESTAMP
                                );`));
                            rows = rows.map(row => [
                                    +row.member_id,
                                    +row.upvotes,
                                    +row.downvotes,
                                    "" + row.is_active
                                ]);
                            res = {
                                status: "OK",
                                data: rows
                            };
                        } catch (err) {
                            res = {
                                status: "ERROR"
                            };
                            // console.error(err);
                        }
                        console.log(JSON.stringify(res));
                    }
                    break;
                default:
                    console.error("Impossible");
            }
        }

        try {
            client.release();
        } catch (err) {}
        try {
            pool.end();
        } catch (err) {}
    })();

}
