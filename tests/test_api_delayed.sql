\set ON_ERROR_STOP on

-- Test delayed delivery via send_at()

-- Test 1: send_at() with future time -> goes to delayed_events
do $$
declare
  v_id bigint;
  v_de_count bigint;
begin
  perform pgque.create_queue('test_delayed');
  perform pgque.register_consumer('test_delayed', 'c1');

  -- Send with future delivery (1 hour from now)
  v_id := pgque.send_at('test_delayed', 'delayed.test',
    '{"delayed":true}'::jsonb, now() + interval '1 hour');
  assert v_id is not null, 'send_at should return id';

  -- Should be in delayed_events, not in queue
  select count(*) into v_de_count from pgque.delayed_events
  where de_queue_name = 'test_delayed';
  assert v_de_count = 1, 'should have 1 delayed event, got ' || v_de_count;

  -- Ticker + receive should return nothing (event is delayed)
  perform pgque.ticker();

  declare v_batch_id bigint;
  begin
    v_batch_id := pgque.next_batch('test_delayed', 'c1');
    if v_batch_id is not null then
      perform pgque.finish_batch(v_batch_id);
    end if;
  end;

  raise notice 'PASS: send_at() with future time goes to delayed_events';

  -- Cleanup delayed events manually
  delete from pgque.delayed_events where de_queue_name = 'test_delayed';
  perform pgque.unregister_consumer('test_delayed', 'c1');
  perform pgque.drop_queue('test_delayed');
end $$;

-- Test 2: send_at() with past time -> goes directly to queue
do $$
declare
  v_id bigint;
  v_de_count bigint;
  v_batch_id bigint;
  v_ev_count int := 0;
begin
  perform pgque.create_queue('test_delayed2');
  perform pgque.register_consumer('test_delayed2', 'c1');

  v_id := pgque.send_at('test_delayed2', 'immediate.test',
    '{"immediate":true}'::jsonb, now() - interval '1 second');
  assert v_id is not null, 'send_at with past time should return event id';

  -- Should NOT be in delayed_events
  select count(*) into v_de_count from pgque.delayed_events
  where de_queue_name = 'test_delayed2';
  assert v_de_count = 0, 'should not be in delayed_events';

  raise notice 'PASS: send_at() with past time goes directly to queue';

  perform pgque.unregister_consumer('test_delayed2', 'c1');
  perform pgque.drop_queue('test_delayed2');
end $$;

-- Test 3: maint_deliver_delayed() moves due events
do $$
declare
  v_count int;
  v_batch_id bigint;
  v_ev record;
begin
  perform pgque.create_queue('test_delayed3');
  perform pgque.register_consumer('test_delayed3', 'c1');

  -- Insert a delayed event that is already due (past time)
  insert into pgque.delayed_events (de_queue_name, de_deliver_at, de_type, de_data)
  values ('test_delayed3', now() - interval '1 second', 'delivered.test', '{"delivered":true}');

  -- Run maint_deliver_delayed
  select pgque.maint_deliver_delayed() into v_count;
  assert v_count >= 1, 'should deliver at least 1 event, got ' || v_count;

  -- Now ticker + receive should get the event
  perform pgque.ticker();
  v_batch_id := pgque.next_batch('test_delayed3', 'c1');

  if v_batch_id is not null then
    for v_ev in select * from pgque.get_batch_events(v_batch_id)
    loop
      assert v_ev.ev_type = 'delivered.test', 'event type should match';
    end loop;
    perform pgque.finish_batch(v_batch_id);
  else
    assert false, 'should have a batch with the delivered event';
  end if;

  raise notice 'PASS: maint_deliver_delayed moves due events';

  perform pgque.unregister_consumer('test_delayed3', 'c1');
  perform pgque.drop_queue('test_delayed3');
end $$;

-- Test 4: pgque.maint() delivers delayed events end-to-end
do $$
declare
  v_delivered int;
begin
  perform pgque.create_queue('test_maint_delayed');
  perform pgque.register_consumer('test_maint_delayed', 'c1');

  -- Insert already-due delayed event
  insert into pgque.delayed_events (de_queue_name, de_deliver_at, de_type, de_data)
  values ('test_maint_delayed', now() - interval '1 second', 'maint.test', '{"via_maint":true}');

  -- Call maint() (the wrapper that pg_cron calls)
  perform pgque.maint();

  -- Verify delayed event was delivered
  assert not exists (
    select 1 from pgque.delayed_events where de_queue_name = 'test_maint_delayed'
  ), 'delayed event should be gone after maint()';

  perform pgque.unregister_consumer('test_maint_delayed', 'c1');
  perform pgque.drop_queue('test_maint_delayed');
  raise notice 'PASS: pgque.maint() delivers delayed events';
end $$;

-- Test 5: grants. The install's blanket revoke from PUBLIC runs before
-- this file, so the file revokes its own functions.
do $$
begin
  assert not has_function_privilege('public', 'pgque.send_at(text, text, jsonb, timestamptz, text, text, text, text)', 'execute'),
    'PUBLIC must not execute send_at';
  assert not has_function_privilege('public', 'pgque.maint_deliver_delayed()', 'execute'),
    'PUBLIC must not execute maint_deliver_delayed';
  assert not has_function_privilege('public', 'pgque.maint()', 'execute'),
    'PUBLIC must not execute maint';
  assert has_function_privilege('pgque_writer', 'pgque.send_at(text, text, jsonb, timestamptz, text, text, text, text)', 'execute'),
    'pgque_writer executes send_at';
  assert not has_function_privilege('pgque_writer', 'pgque.maint_deliver_delayed()', 'execute'),
    'pgque_writer must not execute maint_deliver_delayed';
  assert has_function_privilege('pgque_admin', 'pgque.maint_deliver_delayed()', 'execute'),
    'pgque_admin executes maint_deliver_delayed';
  assert has_function_privilege('pgque_admin', 'pgque.maint()', 'execute'),
    'pgque_admin executes maint';
  raise notice 'PASS: delayed.sql functions are not executable by PUBLIC';
end $$;

-- Test 6: extras are kept through delivery, and used for immediate sends.
-- Separate statements: a tick only sees committed events.
do $$
begin
  perform pgque.create_queue('test_delayed_extras');
  perform pgque.register_consumer('test_delayed_extras', 'c1');
  perform pgque.send_at('test_delayed_extras', 'later', '{}'::jsonb,
    now() + interval '1 hour', null, 'x2', null, 'x4');
  perform pgque.send_at('test_delayed_extras', 'now', '{}'::jsonb,
    now() - interval '1 second', 'n1', null, null, null);
  update pgque.delayed_events set de_deliver_at = now() - interval '1 second'
  where de_queue_name = 'test_delayed_extras';
end $$;

select pgque.maint_deliver_delayed();
select pgque.force_next_tick('test_delayed_extras');
select pgque.ticker();

do $$
declare
  v_batch_id bigint;
  v_ev record;
  v_seen int := 0;
begin
  v_batch_id := pgque.next_batch('test_delayed_extras', 'c1');
  for v_ev in select * from pgque.get_batch_events(v_batch_id) loop
    if v_ev.ev_type = 'later' then
      assert v_ev.ev_extra1 is null and v_ev.ev_extra2 = 'x2' and v_ev.ev_extra4 = 'x4',
        'delivered event keeps its extras';
    else
      assert v_ev.ev_extra1 = 'n1', 'immediate send keeps its extras';
    end if;
    v_seen := v_seen + 1;
  end loop;
  assert v_seen = 2, 'expected 2 events, got ' || v_seen;
  perform pgque.finish_batch(v_batch_id);

  perform pgque.unregister_consumer('test_delayed_extras', 'c1');
  perform pgque.drop_queue('test_delayed_extras');
  raise notice 'PASS: send_at() keeps extras through delivery';
end $$;
