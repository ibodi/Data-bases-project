
CREATE TYPE id_type AS ENUM ('member', 'action', 'project', 'authority');
CREATE TYPE action_type AS ENUM ('support', 'protest');
CREATE TYPE vote_type AS ENUM ('up', 'down');

CREATE TABLE leader_ids (
    id NUMERIC PRIMARY KEY
);

CREATE TABLE all_ids (
    id NUMERIC PRIMARY KEY,
    type id_type NOT null
);

CREATE TABLE members (
    id NUMERIC PRIMARY KEY,
    password_hash TEXT NOT null,
    last_activity TIMESTAMP NOT null,
    upvotes NUMERIC NOT null DEFAULT 0,
    downvotes NUMERIC NOT null DEFAULT 0
);

CREATE TABLE projects (
    id NUMERIC PRIMARY KEY,
    authority_id NUMERIC NOT null,
    creation_time TIMESTAMP NOT null
);

CREATE TABLE actions (
    id NUMERIC PRIMARY KEY,
    project_id NUMERIC NOT null REFERENCES projects(id),
    type action_type NOT null,
    creation_time TIMESTAMP NOT null,
    upvotes NUMERIC NOT null DEFAULT 0,
    downvotes NUMERIC NOT null DEFAULT 0,
    creator_id NUMERIC NOT null REFERENCES members(id)
);

CREATE TABLE votes (
    action_id NUMERIC NOT null REFERENCES actions(id),
    member_id NUMERIC NOT null REFERENCES members(id),
    type vote_type NOT null,
    creation_time TIMESTAMP NOT null
);


CREATE OR REPLACE FUNCTION leader(
        creation_time TIMESTAMP,
        password TEXT,
        member_id NUMERIC
    ) RETURNS VOID AS $x$
    BEGIN
        INSERT INTO all_ids
            VALUES (member_id, 'member');
        INSERT INTO leader_ids
            VALUES (member_id);
        INSERT INTO members
            VALUES (
                member_id,
                crypt(password, gen_salt('bf')),
                creation_time,
                0
            );
    END;
$x$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_leader(
        member_id NUMERIC,
        password TEXT,
        creation_time TIMESTAMP
    ) RETURNS VOID AS $x$
    BEGIN
        IF member_id IN (SELECT id FROM members) AND
            member_id IN (SELECT id FROM leader_ids)
        THEN
            IF NOT EXISTS(SELECT id --
                    FROM members
                    WHERE id=member_id AND
                        password_hash=crypt(password, password_hash) AND
                        creation_time BETWEEN last_activity AND
                            last_activity + interval '1 year'
                )
            THEN
                RAISE EXCEPTION USING
                    errcode='28000',
                    message='Wrong password OR  the member IS NOT active';
            END IF;
            UPDATE members SET last_activity=creation_time
                WHERE id=member_id;
        ELSE
            RAISE EXCEPTION USING
                errcode='28000',
                message='Leader with such member id AND password does NOT exist';
        END IF;
    END;
$x$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_or_create_member(
        member_id NUMERIC,
        password TEXT,
        creation_time TIMESTAMP
    ) RETURNS VOID AS $x$
    BEGIN
        IF member_id IN (SELECT id FROM members)
        THEN
            IF NOT EXISTS(SELECT id
                    FROM members
                    WHERE id=member_id AND
                        password_hash=crypt(password, password_hash) AND
                        creation_time BETWEEN last_activity AND
                            last_activity + interval '1 year'
                )
            THEN
                RAISE EXCEPTION USING
                    errcode='28000',
                    message='Wrong password OR  the member IS NOT active';
            END IF;
            UPDATE members SET last_activity=creation_time
                WHERE id=member_id;
        ELSE
            INSERT INTO all_ids
                VALUES (member_id, 'member');
            INSERT INTO members
                VALUES (
                    member_id,
                    crypt(password, gen_salt('bf')),
                    creation_time,
                    0
                );
        END IF;
    END;
$x$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_action (
        creation_time TIMESTAMP,
        member_id NUMERIC, password TEXT,
        action_id NUMERIC, project_id NUMERIC,
        is_support boolean,
        authority_id NUMERIC DEFAULT null
    ) RETURNS VOID AS $x$
    DECLARE help TEXT;
    BEGIN
        SELECT validate_or_create_member(member_id, password, creation_time)
            INTO help;

        IF project_id NOT IN (SELECT id FROM projects)
        THEN
            IF authority_id IS null THEN
                RAISE EXCEPTION USING
                    errcode='54023',
                    message='Project with such project id IS NOT IN the' ||
                        ' database. Authority id must be provided';
            END IF;
            INSERT INTO all_ids
                VALUES (project_id, 'project');
            IF authority_id NOT IN (SELECT id FROM all_ids)
            THEN
                INSERT INTO all_ids
                    VALUES (authority_id, 'authority');
            END IF;
            INSERT INTO projects
                VALUES (project_id, authority_id, creation_time);
        END IF;

        INSERT INTO all_ids
            VALUES (action_id, 'action');
        IF is_support
        THEN
            INSERT INTO actions
                VALUES (action_id, project_id, 'support',
                    creation_time, 0, 0, member_id);
        ELSE
            INSERT INTO actions
                VALUES (action_id, project_id, 'protest',
                    creation_time, 0, 0, member_id);
        END IF;
    END;
$x$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION support(
        creation_time TIMESTAMP,
        member_id NUMERIC, password TEXT,
        action_id NUMERIC, project_id NUMERIC,
        authority_id NUMERIC DEFAULT null
    ) RETURNS VOID AS $x$
    DECLARE help TEXT;
    BEGIN
        SELECT create_action(creation_time,
                member_id, password,
                action_id, project_id,
                TRUE, authority_id)
        INTO help;
    END;
$x$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION protest(
        creation_time TIMESTAMP,
        member_id NUMERIC, password TEXT,
        action_id NUMERIC, project_id NUMERIC,
        authority_id NUMERIC DEFAULT null
    ) RETURNS VOID AS $x$
    DECLARE help TEXT;
    BEGIN
        SELECT create_action(creation_time,
                member_id, password,
                action_id, project_id,
                FALSE, authority_id)
        INTO help;
    END;
$x$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION vote(
        creation_time TIMESTAMP,
        member_id NUMERIC, password TEXT,
        action_id NUMERIC, is_upvote boolean
    ) RETURNS VOID AS $x$
    DECLARE help TEXT;
    BEGIN
        SELECT validate_or_create_member(member_id, password, creation_time)
        INTO help;

        IF action_id NOT IN (SELECT id FROM actions) THEN
            RAISE EXCEPTION USING
                errcode='02000',
                message='Action with such action id does NOT exist';
        END IF;

        IF EXISTS(SELECT *
            FROM votes
            WHERE votes.member_id=$2 AND
                votes.action_id=$4
        ) THEN
            RAISE EXCEPTION USING
                errcode='42P05',
                message='This member has already voted for OR against' ||
                    ' this action';
        END IF;

        IF is_upvote THEN
            INSERT INTO votes
                VALUES (action_id, member_id, 'up', creation_time);
            UPDATE actions SET upvotes=upvotes+1
                WHERE id=action_id;
            UPDATE members SET upvotes=upvotes+1
                WHERE members.id IN (SELECT creator_id
                    FROM actions
                    WHERE actions.id=action_id
                );
        ELSE
            INSERT INTO votes
                VALUES (action_id, member_id, 'down', creation_time);
            UPDATE actions SET downvotes=downvotes+1
                WHERE id=action_id;
            UPDATE members SET downvotes=downvotes+1
                WHERE members.id IN (SELECT creator_id
                    FROM actions
                    WHERE actions.id=action_id
                );
        END IF;
    END;
$x$ LANGUAGE plpgsql;

CREATE OR  REPLACE FUNCTION upvote(
        creation_time TIMESTAMP, member_id NUMERIC,
        password TEXT, action_id NUMERIC
    ) RETURNS VOID AS $x$
    DECLARE help TEXT;
    BEGIN
        SELECT vote(creation_time, member_id, password, action_id, TRUE)
            INTO help;
    END;
$x$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION downvote(
        creation_time TIMESTAMP, member_id NUMERIC,
        password TEXT, action_id NUMERIC
    ) RETURNS VOID AS $x$
    DECLARE help TEXT;
    BEGIN
        SELECT vote(creation_time, member_id, password, action_id, FALSE)
            INTO help;
    END;
$x$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_actions(
        creation_time TIMESTAMP,
        member_id NUMERIC, password TEXT,
        _type action_type DEFAULT null,
        project_or_authority id_type DEFAULT null,
        project_or_authority_id NUMERIC DEFAULT null
    ) RETURNS TABLE (
            action_id NUMERIC,
            type TEXT,
            project_id NUMERIC,
            authority_id NUMERIC,
            upvotes NUMERIC,
            downvotes NUMERIC
        ) AS $x$
    DECLARE help TEXT;
            var_r record;
    BEGIN
        SELECT validate_leader(member_id, password, creation_time)
            INTO help;

        IF project_or_authority IS NOT null AND
            project_or_authority_id IS null
        THEN
            RAISE EXCEPTION USING
                errcode='22004',
                message='Information about whether we should filter data' ||
                    ' by project OR  authority IS provided but no id of' ||
                    ' project OR  authority IS provided';
        END IF;

        IF _type IS null AND project_or_authority IS null
        THEN
            RETURN QUERY
                SELECT
                    actions.id, actions.type::TEXT,
                    actions.project_id, projects.authority_id,
                    actions.upvotes, actions.downvotes
                FROM actions
                JOIN projects ON actions.project_id=projects.id
                ORDER BY actions.id ASC;
        ELSIF _type IS NOT null AND project_or_authority IS null
        THEN
            RETURN QUERY
                SELECT
                    actions.id, actions.type::TEXT,
                    actions.project_id, projects.authority_id,
                    actions.upvotes, actions.downvotes
                FROM actions
                JOIN projects ON actions.project_id=projects.id
                WHERE
                    _type=actions.type
                ORDER BY actions.id ASC;
        ELSIF _type IS NOT null AND project_or_authority IS NOT null
        THEN
            IF project_or_authority='project'
            THEN
                RETURN QUERY
                    SELECT
                        actions.id, actions.type::TEXT,
                        actions.project_id, projects.authority_id,
                        actions.upvotes, actions.downvotes
                    FROM actions
                    JOIN projects ON actions.project_id=projects.id
                    WHERE
                        _type=actions.type AND
                        actions.project_id=project_or_authority_id
                    ORDER BY actions.id ASC;
            ELSIF project_or_authority='authority'
            THEN
                RETURN QUERY
                    SELECT
                        actions.id, actions.type::TEXT,
                        actions.project_id, projects.authority_id,
                        actions.upvotes, actions.downvotes
                    FROM actions
                    JOIN projects ON actions.project_id=projects.id
                    WHERE
                        _type=actions.type AND
                        projects.authority_id=project_or_authority_id
                    ORDER BY actions.id ASC;
            ELSE
                RAISE EXCEPTION USING
                    errcode='42804',
                    message='Wrong id_type of last parameter IS given';
            END IF;
        ELSIF _type IS null AND project_or_authority IS NOT null
        THEN
            IF project_or_authority='project'
            THEN
                RETURN QUERY
                    SELECT
                        actions.id, actions.type::TEXT,
                        actions.project_id, projects.authority_id,
                        actions.upvotes, actions.downvotes
                    FROM actions
                    JOIN projects ON actions.project_id=projects.id
                    WHERE --creation_time=actions.creation_time AND
                        actions.project_id=project_or_authority_id
                    ORDER BY actions.id ASC;
            ELSIF project_or_authority='authority'
            THEN
                RETURN QUERY
                    SELECT
                        actions.id, actions.type::TEXT,
                        actions.project_id, projects.authority_id,
                        actions.upvotes, actions.downvotes
                    FROM actions
                    JOIN projects ON actions.project_id=projects.id
                    WHERE --creation_time=actions.creation_time AND
                        projects.authority_id=project_or_authority_id
                    ORDER BY actions.id ASC;
            ELSE
                RAISE EXCEPTION USING
                    errcode='42804',
                    message='Wrong id_type of last argument IS given';
            END IF;
        END IF;
    END;
$x$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_projects(
        creation_time TIMESTAMP,
        member_id NUMERIC, password TEXT,
        _authority_id NUMERIC DEFAULT null
    ) RETURNS TABLE (
            project_id NUMERIC,
            authority_id NUMERIC
        ) AS $x$
    DECLARE help TEXT;
    BEGIN
        SELECT validate_leader(member_id, password, creation_time)
        INTO help;
        IF _authority_id IS null
        THEN
            RETURN QUERY
                SELECT id, projects.authority_id
                FROM projects
                ORDER BY id ASC;
        ELSE
            RETURN QUERY
                SELECT id, projects.authority_id
                FROM projects
                WHERE projects.authority_id=_authority_id
                ORDER BY id ASC;
        END IF;
    END;
$x$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_votes(
        creation_time TIMESTAMP,
        _member_id NUMERIC, password TEXT,
        action_or_project id_type DEFAULT null,
        action_or_project_id NUMERIC DEFAULT null
    ) RETURNS TABLE (
            member_id NUMERIC,
            upvotes NUMERIC,
            downvotes NUMERIC
        ) AS $x$
    DECLARE help TEXT;
    BEGIN
        IF action_or_project IS NOT null AND
            action_or_project_id IS null
        THEN
            RAISE EXCEPTION USING
                errcode='22004',
                message='Information about whether we should filter data' ||
                    ' by action OR  project IS provided but no id of action' ||
                    ' OR  project IS provided';
        END IF;

        SELECT validate_leader(_member_id, password, creation_time)
            INTO help;

        IF action_or_project IS null
        THEN
            RETURN QUERY
                SELECT members.id,
                    CASE
                        WHEN up.upvotes IS null THEN 0::NUMERIC
                        ELSE  up.upvotes
                    END AS upvotes,
                    CASE
                        WHEN down.downvotes IS null THEN 0::NUMERIC
                        ELSE  down.downvotes
                    END AS downvotes
                FROM
                members
                LEFT JOIN
                (SELECT votes.member_id, COUNT(*) AS upvotes
                    FROM votes
                    WHERE type='up'
                    GROUP BY votes.member_id) AS up
                ON members.id=up.member_id
                LEFT JOIN
                (SELECT votes.member_id, COUNT(*) AS downvotes
                    FROM votes
                    WHERE type='down'
                    GROUP BY votes.member_id) AS down
                ON members.id=down.member_id
                ORDER BY members.id ASC;
        ELSIF action_or_project='action'
        THEN
            RETURN QUERY
                SELECT members.id,
                    CASE
                        WHEN up.upvotes IS null THEN 0::NUMERIC
                        ELSE up.upvotes
                    END AS upvotes,
                    CASE
                        WHEN down.downvotes IS null THEN 0::NUMERIC
                        ELSE down.downvotes
                    END AS downvotes
                FROM
                members
                LEFT JOIN
                (SELECT votes.member_id, COUNT(*) AS upvotes
                    FROM votes
                    WHERE votes.type='up' AND
                        votes.action_id=action_or_project_id
                    GROUP BY votes.member_id) AS up
                ON members.id=up.member_id
                LEFT JOIN
                (SELECT votes.member_id, COUNT(*) AS downvotes
                    FROM votes
                    WHERE votes.type='down' AND
                        votes.action_id=action_or_project_id
                    GROUP BY votes.member_id) AS down
                ON members.id=down.member_id
                ORDER BY members.id ASC;
        ELSIF action_or_project='project'
        THEN
            RETURN QUERY
                SELECT members.id,
                    CASE
                        WHEN up.upvotes IS null THEN 0::NUMERIC
                        ELSE  up.upvotes
                    END AS upvotes,
                    CASE
                        WHEN down.downvotes IS null THEN 0::NUMERIC
                        ELSE  down.downvotes
                    END AS downvotes
                FROM
                members
                LEFT JOIN
                (SELECT votes.member_id, COUNT(*) AS upvotes
                    FROM votes
                    JOIN actions
                    ON actions.id=votes.action_id
                    WHERE votes.type='up' AND
                        actions.project_id=action_or_project_id
                    GROUP BY votes.member_id) AS up
                ON members.id=up.member_id
                LEFT JOIN
                (SELECT votes.member_id, COUNT(*) AS downvotes
                    FROM votes
                    JOIN actions
                    ON actions.id=votes.action_id
                    WHERE votes.type='down' AND
                        actions.project_id=action_or_project_id
                    GROUP BY votes.member_id) AS down
                ON members.id=down.member_id
                ORDER BY members.id ASC;
        ELSE
            RAISE EXCEPTION USING
                errcode='42804',
                message='Wrong id_type of forth argument IS given';
        END IF;
    END;
$x$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_trolls(
        creation_time TIMESTAMP
    ) RETURNS TABLE (
        member_id NUMERIC,
        upvotes NUMERIC,
        downvotes NUMERIC,
        is_active boolean
    ) AS $x$
    BEGIN
        RETURN QUERY
            SELECT
                id, members.upvotes,
                members.downvotes,
                (creation_time BETWEEN last_activity AND
                    last_activity + interval '1 year')
            FROM members
            WHERE members.downvotes-members.upvotes>0
            ORDER BY members.downvotes-members.upvotes DESC,
                id ASC;
    END;
$x$ LANGUAGE plpgsql;

CREATE USER app WITH ENCRYPTED PASSWORD 'qwerty';

GRANT EXECUTE ON FUNCTION validate_leader(NUMERIC, TEXT, TIMESTAMP) TO app;
GRANT EXECUTE ON FUNCTION validate_or_create_member(NUMERIC, TEXT,
    TIMESTAMP) TO app;
GRANT EXECUTE ON FUNCTION create_action(TIMESTAMP, NUMERIC, TEXT,
    NUMERIC, NUMERIC, boolean, NUMERIC) TO app;
GRANT EXECUTE ON FUNCTION support(TIMESTAMP, NUMERIC, TEXT, NUMERIC,
    NUMERIC, NUMERIC) TO app;
GRANT EXECUTE ON FUNCTION protest(TIMESTAMP, NUMERIC, TEXT, NUMERIC,
    NUMERIC, NUMERIC) TO app;
GRANT EXECUTE ON FUNCTION vote(TIMESTAMP, NUMERIC, TEXT, NUMERIC,
    boolean) TO app;
GRANT EXECUTE ON FUNCTION upvote(TIMESTAMP, NUMERIC, TEXT, NUMERIC) TO app;
GRANT EXECUTE ON FUNCTION downvote(TIMESTAMP, NUMERIC, TEXT, NUMERIC) TO app;
GRANT EXECUTE ON FUNCTION get_actions(TIMESTAMP, NUMERIC, TEXT, action_type,
    id_type, NUMERIC) TO app;
GRANT EXECUTE ON FUNCTION get_projects(TIMESTAMP, NUMERIC, TEXT, NUMERIC) TO app;
GRANT EXECUTE ON FUNCTION get_votes(TIMESTAMP, NUMERIC, TEXT, id_type,
    NUMERIC) TO app;
GRANT EXECUTE ON FUNCTION get_trolls(TIMESTAMP) TO app;

GRANT SELECT, INSERT ON TABLE all_ids TO app;
GRANT SELECT, INSERT ON TABLE projects TO app;
GRANT SELECT, INSERT ON TABLE leader_ids TO app;
GRANT SELECT, INSERT ON TABLE votes TO app;
GRANT SELECT, INSERT, UPDATE ON TABLE actions TO app;
GRANT SELECT, INSERT, UPDATE ON TABLE members TO app;


-- -- USEFULL COMMANDS: remove and create db bazy and user app.
-- \c ibodi
-- DROP DATABASE bazy;
-- DROP USER app;
-- CREATE DATABASE bazy;
-- GRANT ALL PRIVILEGES ON DATABASE bazy TO init;
-- \c bazy
-- CREATE EXTENSION pgcrypto;
