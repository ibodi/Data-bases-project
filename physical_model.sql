
create type id_type as enum ('member', 'action', 'project', 'authority');
create type action_type as enum ('support', 'protest');
create type vote_type as enum ('up', 'down');

create extension pgcrypto;

create table leader_ids (
    id numeric primary key
);

create table all_ids (
    id numeric primary key,
    type id_type not null
);

create table members (
    id numeric primary key,
    password_hash text not null,
    last_activity timestamp not null,
    upvotes numeric not null default 0,
    downvotes numeric not null default 0
);

create table actions (
    id numeric primary key,
    project_id numeric not null,
    type action_type not null,
    creation_time timestamp not null,
    upvotes numeric not null default 0,
    downvotes numeric not null default 0,
    creator_id numeric not null
);

create table votes (
    action_id numeric not null,
    member_id numeric not null,
    type vote_type not null,
    creation_time timestamp not null
);

create table projects (
    id numeric primary key, -- serial? primary key?
    authority_id numeric not null,
    creation_time timestamp not null
);


create or replace function leader(
        creation_time timestamp,
        password text,
        member_id numeric
    ) returns void as $x$
    begin
        insert into all_ids
            values (member_id, 'member');
        insert into leader_ids
            values (member_id);
        insert into members
            values (
                member_id,
                crypt(password, gen_salt('bf')),
                creation_time,
                0
            );
    end;
$x$ language plpgsql;

create or replace function validate_leader(
        member_id numeric,
        password text,
        creation_time timestamp
    ) returns void as $x$
    begin
        if member_id in (select id from members) and
            member_id in (select id from leader_ids)
        then
            if not exists(select id --
                    from members
                    where id=member_id and
                        password_hash=crypt(password, password_hash) and
                        creation_time-last_activity<interval '1 year'
                )
            then
                raise exception using
                    errcode='28000',
                    message='Wrong password or the member is not active';
            end if;
            update members set last_activity=creation_time
                where id=member_id;
        else
            raise exception using
                errcode='28000',
                message='Leader with such member id and password does not exist';
        end if;
    end;
$x$ language plpgsql;

create or replace function validate_or_create_member(
        member_id numeric,
        password text,
        creation_time timestamp
    ) returns void as $x$
    begin
        if member_id in (select id from members)
        then
            if not exists(select id --
                    from members
                    where id=member_id and
                        password_hash=crypt(password, password_hash) and
                        creation_time-last_activity<interval '1 year'
                )
            then
                raise exception using
                    errcode='28000',
                    message='Wrong password or the member is not active';
            end if;
            update members set last_activity=creation_time
                where id=member_id;
        else
            insert into all_ids
                values (member_id, 'member');
            insert into members
                values (
                    member_id,
                    crypt(password, gen_salt('bf')),
                    creation_time,
                    0
                );
        end if;
    end;
$x$ language plpgsql;

create or replace function create_action (
        creation_time timestamp,
        member_id numeric, password text,
        action_id numeric, project_id numeric,
        is_support boolean,
        authority_id numeric default null
    ) returns void as $x$
    declare help text;
    begin
        select validate_or_create_member(member_id, password, creation_time) into help;

        insert into all_ids
            values (action_id, 'action');
        if is_support
        then
            insert into actions
                values (action_id, project_id, 'support',
                    creation_time, 0, 0, member_id);
        else
            insert into actions
                values (action_id, project_id, 'protest',
                    creation_time, 0, 0, member_id);
        end if;

        if project_id not in (select id from projects)
        then
            if authority_id is null then
                raise exception using
                    errcode='54023',
                    message='Project with such project id is not in the database. Authority id must be provided';
            end if;
            insert into all_ids
                values (project_id, 'project');
            if authority_id not in (select id from all_ids)
            then
                insert into all_ids
                    values (authority_id, 'authority');
            end if;
            insert into projects
                values (project_id, authority_id, creation_time);
        end if;
    end;
$x$ language plpgsql;

create or replace function support(
        creation_time timestamp,
        member_id numeric, password text,
        action_id numeric, project_id numeric,
        authority_id numeric default null
    ) returns void as $x$
    declare help text;
    begin
        select create_action(creation_time,
                member_id, password,
                action_id, project_id,
                TRUE, authority_id)
        into help;
    end;
$x$ language plpgsql;

create or replace function protest(
        creation_time timestamp,
        member_id numeric, password text,
        action_id numeric, project_id numeric,
        authority_id numeric default null
    ) returns void as $x$
    declare help text;
    begin
        select create_action(creation_time,
                member_id, password,
                action_id, project_id,
                FALSE, authority_id)
        into help;
    end;
$x$ language plpgsql;

create or replace function vote(
        creation_time timestamp,
        member_id numeric, password text,
        action_id numeric, is_upvote boolean
    ) returns void as $x$
    declare help text;
    begin
        select validate_or_create_member(member_id, password, creation_time)
        into help;

        if action_id not in (select id from actions) then
            raise exception using
                errcode='02000',
                message='Action with such action id does not exist';
        end if;

        if exists(select *
            from votes
            where votes.member_id=$2 and
                votes.action_id=$4
        ) then
            raise exception using
                errcode='42P05',
                message='This member has already voted for or against this action';
        end if;

        if is_upvote then
            insert into votes
                values (action_id, member_id, 'up', creation_time);
            update actions set upvotes=upvotes+1
                where id=action_id;
            update members set upvotes=upvotes+1
                where members.id in (select creator_id
                    from actions
                    where actions.id=action_id
                );
        else
            insert into votes
                values (action_id, member_id, 'down', creation_time);
            update actions set downvotes=downvotes+1
                where id=action_id;
            update members set downvotes=downvotes+1
                where members.id in (select creator_id
                    from actions
                    where actions.id=action_id
                );
        end if;
    end;
$x$ language plpgsql;

create or replace function upvote(
        creation_time timestamp, member_id numeric,
        password text, action_id numeric
    ) returns void as $x$
    declare help text;
    begin
        select vote(creation_time, member_id, password, action_id, TRUE)
        into help;
    end;
$x$ language plpgsql;

create or replace function downvote(
        creation_time timestamp, member_id numeric,
        password text, action_id numeric
    ) returns void as $x$
    declare help text;
    begin
        select vote(creation_time, member_id, password, action_id, FALSE)
        into help;
    end;
$x$ language plpgsql;

create or replace function get_actions(
        creation_time timestamp,
        member_id numeric, password text,
        _type action_type default null,
        project_or_authority id_type default null,
        project_or_authority_id numeric default null
    ) returns table (
            action_id numeric,
            type text,
            project_id numeric,
            authority_id numeric,
            upvotes numeric,
            downvotes numeric
        ) as $x$
    declare help text;
    begin
        select validate_leader(member_id, password, creation_time)
        into help;

        if project_or_authority is not null and
            project_or_authority_id is null
        then
            raise exception using
                errcode='22004',
                message='Information about whether we should filter data by project or authority is provided but no id of project or authority is provided';
        end if;

        if _type is null and project_or_authority is null
        then
            return query
                select
                    actions.id, actions.type::text,
                    actions.project_id, projects.authority_id,
                    actions.upvotes, actions.downvotes
                from actions
                join projects on actions.project_id=projects.id
                order by actions.id asc;
                --where creation_time=actions.creation_time; -- this requirement isn't present in the documentation
        elsif _type is not null and project_or_authority is null
        then
            return query
                select
                    actions.id, actions.type::text,
                    actions.project_id, projects.authority_id,
                    actions.upvotes, actions.downvotes
                from actions
                join projects on actions.project_id=projects.id
                where --creation_time=actions.creation_time and
                    _type=actions.type
                order by actions.id asc;
        elsif _type is not null and project_or_authority is not null
        then
            if project_or_authority='project'
            then
                return query
                    select
                        actions.id, actions.type::text,
                        actions.project_id, projects.authority_id,
                        actions.upvotes, actions.downvotes
                    from actions
                    join projects on actions.project_id=projects.id
                    where --creation_time=actions.creation_time and
                        _type=actions.type and
                        actions.project_id=project_or_authority_id
                    order by actions.id asc;
            elsif project_or_authority='authority'
            then
                return query
                    select
                        actions.id, actions.type::text,
                        actions.project_id, projects.authority_id,
                        actions.upvotes, actions.downvotes
                    from actions
                    join projects on actions.project_id=projects.id
                    where --creation_time=actions.creation_time and
                        _type=actions.type and
                        projects.authority_id=project_or_authority_id
                    order by actions.id asc;
            else
                raise exception using
                    errcode='42804',
                    message='Wrong id_type of last parameter is given';
            end if;
        elsif _type is null and project_or_authority is not null
        then
            if project_or_authority='project'
            then
                return query
                    select
                        actions.id, actions.type::text,
                        actions.project_id, projects.authority_id,
                        actions.upvotes, actions.downvotes
                    from actions
                    join projects on actions.project_id=projects.id
                    where --creation_time=actions.creation_time and
                        actions.project_id=project_or_authority_id
                    order by actions.id asc;
            elsif project_or_authority='authority'
            then
                return query
                    select
                        actions.id, actions.type::text,
                        actions.project_id, projects.authority_id,
                        actions.upvotes, actions.downvotes
                    from actions
                    join projects on actions.project_id=projects.id
                    where --creation_time=actions.creation_time and
                        projects.authority_id=project_or_authority_id
                    order by actions.id asc;
            else
                raise exception using
                    errcode='42804',
                    message='Wrong id_type of last argument is given';
            end if;
        end if;
    end;
$x$ language plpgsql;

create or replace function get_projects(
        creation_time timestamp,
        member_id numeric, password text,
        _authority_id numeric default null
    ) returns table (
            project_id numeric,
            authority_id numeric
        ) as $x$
    declare help text;
    begin
        select validate_leader(member_id, password, creation_time)
        into help;
        if _authority_id is null
        then
            return query
                select id, projects.authority_id
                from projects
                order by id asc;
        else
            return query
                select id, projects.authority_id
                from projects
                where projects.authority_id=_authority_id
                order by id asc;
        end if;
    end;
$x$ language plpgsql;

create or replace function get_votes(
        creation_time timestamp,
        _member_id numeric, password text,
        action_or_project id_type default null,
        action_or_project_id numeric default null
    ) returns table (
            member_id numeric,
            upvotes numeric,
            downvotes numeric
        ) as $x$
    declare help text;
    begin
        if action_or_project is not null and
            action_or_project_id is null
        then
            raise exception using
                errcode='22004',
                message='Information about whether we should filter data by action or project is provided but no id of action or project is provided';
        end if;

        select validate_leader(_member_id, password, creation_time)
        into help;

        if action_or_project is null
        then
            return query
                select members.id,
                    case
                        when up.upvotes is null then 0::numeric
                        else up.upvotes
                    end as upvotes,
                    case
                        when down.downvotes is null then 0::numeric
                        else down.downvotes
                    end as downvotes
                from
                members
                left join
                (select votes.member_id, count(*) as upvotes
                    from votes
                    where type='up'
                    group by votes.member_id) as up
                on members.id=up.member_id
                left join
                (select votes.member_id, count(*) as downvotes
                    from votes
                    where type='down'
                    group by votes.member_id) as down
                on members.id=down.member_id
                order by members.id asc;
        elsif action_or_project='action'
        then
            return query
                select members.id,
                    case
                        when up.upvotes is null then 0::numeric
                        else up.upvotes
                    end as upvotes,
                    case
                        when down.downvotes is null then 0::numeric
                        else down.downvotes
                    end as downvotes
                from
                members
                left join
                (select votes.member_id, count(*) as upvotes
                    from votes
                    where votes.type='up' and
                        votes.action_id=action_or_project_id
                    group by votes.member_id) as up
                on members.id=up.member_id
                left join
                (select votes.member_id, count(*) as downvotes
                    from votes
                    where votes.type='down' and
                        votes.action_id=action_or_project_id
                    group by votes.member_id) as down
                on members.id=down.member_id
                order by members.id asc;
        elsif action_or_project='project'
        then
            return query
                select members.id,
                    case
                        when up.upvotes is null then 0::numeric
                        else up.upvotes
                    end as upvotes,
                    case
                        when down.downvotes is null then 0::numeric
                        else down.downvotes
                    end as downvotes
                from
                members
                left join
                (select votes.member_id, count(*) as upvotes
                    from votes
                    join actions
                    on actions.id=votes.action_id
                    where votes.type='up' and
                        actions.project_id=action_or_project_id
                    group by votes.member_id) as up
                on members.id=up.member_id
                left join
                (select votes.member_id, count(*) as downvotes
                    from votes
                    join actions
                    on actions.id=votes.action_id
                    where votes.type='down' and
                        actions.project_id=action_or_project_id
                    group by votes.member_id) as down
                on members.id=down.member_id
                order by members.id asc;
        else
            raise exception using
                errcode='42804',
                message='Wrong id_type of forth argument is given';
        end if;
    end;
$x$ language plpgsql;

create or replace function get_trolls(
        creation_time timestamp
    ) returns table (
        member_id numeric,
        upvotes numeric,
        downvotes numeric,
        is_active boolean
    ) as $x$
    begin
        return query
            select
                id, members.upvotes,
                members.downvotes,
                creation_time-last_activity<interval '1 year'
            from members
            order by members.downvotes-members.upvotes desc,
                id asc;
    end;
$x$ language plpgsql;
