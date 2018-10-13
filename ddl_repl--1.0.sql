/* share\postgresql\extension  pg_ctl  start -D C:\Users\realy\Documents\PostgreSQLData1 */ 


CREATE SCHEMA ddl_repl;

CREATE ROLE ddl_repl; /* add this role to the user which DDL commands should be replicated - only when GUC only_repl_users is enabled */

create table ddl_repl.nodes
(
    id bigserial not null primary key,
	node_name text not null unique,
	dsn text not null,
	description text,
	active boolean not null default true,
	creation_date timestamp not null default now()
);

create function ddl_repl.create_node(
	node_name text,
	dsn text,
	description text DEFAULT NULL,
	active boolean DEFAULT TRUE
) 
RETURNS void
as
$func$
begin
  if trim(coalesce(node_name, '')) = '' then
    raise 'node_name not specified';
  elsif trim(coalesce(dsn, '')) = '' then 
    raise 'dsn not specified';
  end if;
  insert into ddl_repl.nodes (node_name, dsn, description, active)
  values (node_name, dsn, description, active);
exception
	when unique_violation then
	 raise 'Node % already exists', node_name;
	when others then
	 raise;
end;
$func$ LANGUAGE plpgsql;

create or replace function ddl_repl.drop_node(
  node_name text
) 
returns void
as
$func$
declare
  deleted_name text;
begin
 if trim(coalesce($1, '')) = '' then
    raise 'node_name not specified';
  end if;
  delete from ddl_repl.nodes n where n.node_name = $1 returning n.node_name into deleted_name;
  if deleted_name is null then 
    raise 'Node % not found', $1;
  end if;
end;
$func$ LANGUAGE plpgsql;


create or replace function ddl_repl.set_node_active(
  node_name text,
  active boolean
) 
returns void
as
$func$
declare
  changed_node text;
begin
 if trim(coalesce($1, '')) = '' then
    raise 'node_name not specified';
  end if;
  update ddl_repl.nodes n set active = $2 where n.node_name = $1 returning n.node_name into changed_node;
  if changed_node is null then 
    raise 'Node % not found', $1;
  end if;
end;
$func$ LANGUAGE plpgsql;