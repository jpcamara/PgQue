\set ON_ERROR_STOP on

-- Test: receive() and receive_coop() skip an empty batch when a later
-- batch is ready.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- An empty batch (a tick window with no visible events) is finished and
-- returns no rows, which is also what receive() returns when no batch is
-- ready. A consumer that sees no rows waits for the next NOTIFY or poll,
-- so when a later batch was already ready, its events sat until then.
-- receive() must finish the empty batch and return the next one.

-- Plain consumer
do $$
begin
  perform pgque.create_queue('test_skip_empty');
  perform pgque.register_consumer('test_skip_empty', 'c1');
end $$;

-- An empty tick window, then a window with one event.
do $$
begin
  perform pgque.force_next_tick('test_skip_empty');
  perform pgque.ticker();
end $$;

select pgque.send('test_skip_empty', 'after', 'empty');

do $$
begin
  perform pgque.force_next_tick('test_skip_empty');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
  v_batch bigint;
begin
  for v_msg in select * from pgque.receive('test_skip_empty', 'c1', 100)
  loop
    v_count := v_count + 1;
    v_batch := v_msg.batch_id;
  end loop;

  assert v_count = 1,
    'receive() should skip the empty batch and return the ready event, got '
    || v_count || ' rows';
  assert v_msg.type = 'after', 'unexpected type: ' || coalesce(v_msg.type, 'NULL');

  perform pgque.ack(v_batch);
  raise notice 'PASS: receive() skips an empty batch when a later batch is ready';
end $$;

do $$
begin
  perform pgque.unregister_consumer('test_skip_empty', 'c1');
  perform pgque.drop_queue('test_skip_empty');
end $$;

-- Cooperative subconsumer
do $$
begin
  perform pgque.create_queue('coop_skip_empty');
  perform pgque.register_subconsumer('coop_skip_empty', 'main_c', 'w1');
end $$;

do $$
begin
  perform pgque.force_next_tick('coop_skip_empty');
  perform pgque.ticker();
end $$;

select pgque.send('coop_skip_empty', 'after', 'empty');

do $$
begin
  perform pgque.force_next_tick('coop_skip_empty');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
  v_batch bigint;
begin
  for v_msg in select * from pgque.receive_coop('coop_skip_empty', 'main_c', 'w1', 100)
  loop
    v_count := v_count + 1;
    v_batch := v_msg.batch_id;
  end loop;

  assert v_count = 1,
    'receive_coop() should skip the empty batch and return the ready event, got '
    || v_count || ' rows';
  assert v_msg.type = 'after', 'unexpected type: ' || coalesce(v_msg.type, 'NULL');

  perform pgque.ack(v_batch);
  raise notice 'PASS: receive_coop() skips an empty batch when a later batch is ready';
end $$;

do $$
begin
  perform pgque.unregister_subconsumer('coop_skip_empty', 'main_c', 'w1', 1);
  perform pgque.drop_queue('coop_skip_empty', true);
end $$;
