\set ON_ERROR_STOP on

-- Test: pgque.send_batch(queue, types, payloads, extra1..4)
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
begin
  perform pgque.create_queue('sbx_q');
  perform pgque.register_consumer('sbx_q', 'c1');
end $$;

do $$
declare
  v_ids bigint[];
begin
  v_ids := pgque.send_batch('sbx_q', array['a', 'b'],
    array['{"i":1}', '{"i":2}']::jsonb[],
    null, array['x', null], null, array['y1', 'y2']);
  assert cardinality(v_ids) = 2, 'expected 2 ids';

  -- The existing three-argument form still resolves with an untyped literal.
  v_ids := pgque.send_batch('sbx_q', 'c', array['{"i":3}']::jsonb[]);
  assert cardinality(v_ids) = 1, 'three-argument send_batch still works';

  assert pgque.send_batch('sbx_q', array[]::text[], array[]::jsonb[], null, null, null, null) = '{}',
    'empty payloads return no ids';
  raise notice 'PASS: send_batch() with per-event types and extras sends';
end $$;

do $$
declare
  v_batch bigint;
  v_ev record;
  v_seen text[] := '{}';
begin
  perform pgque.force_next_tick('sbx_q');
  perform pgque.ticker();
  v_batch := pgque.next_batch('sbx_q', 'c1');
  for v_ev in select * from pgque.get_batch_events(v_batch) order by ev_id loop
    v_seen := v_seen || format('%s:%s:%s:%s:%s', v_ev.ev_type, v_ev.ev_extra1, v_ev.ev_extra2, v_ev.ev_extra3, v_ev.ev_extra4);
  end loop;
  assert v_seen = array['a::x::y1', 'b::::y2', 'c::::'],
    'unexpected events: ' || v_seen::text;
  perform pgque.finish_batch(v_batch);
  raise notice 'PASS: send_batch() stores each event''s type and extras in order';
end $$;

do $$
begin
  begin
    perform pgque.send_batch('sbx_q', array['a'], array['{}', '{}']::jsonb[], null, null, null, null);
    assert false, 'mismatched types must raise';
  exception when raise_exception then
    assert sqlerrm like 'type_names must have one entry per payload%', sqlerrm;
  end;
  begin
    perform pgque.send_batch('sbx_q', array['a', 'a'], array['{}', '{}']::jsonb[], array['x'], null, null, null);
    assert false, 'mismatched extras must raise';
  exception when raise_exception then
    assert sqlerrm like 'extras arrays must be NULL or have one entry per payload%', sqlerrm;
  end;
  raise notice 'PASS: send_batch() rejects arrays of the wrong length';
end $$;

do $$
begin
  assert has_function_privilege('pgque_writer',
    'pgque.send_batch(text, text[], jsonb[], text[], text[], text[], text[])', 'execute'),
    'pgque_writer executes the new send_batch';
  assert not has_function_privilege('public',
    'pgque.send_batch(text, text[], jsonb[], text[], text[], text[], text[])', 'execute'),
    'PUBLIC must not execute the new send_batch';
  assert not has_function_privilege('pgque_admin',
    'pgque.insert_event_bulk(text, text[], text[], text[], text[], text[], text[])', 'execute'),
    'the bulk primitive stays internal';
  raise notice 'PASS: send_batch() with extras is granted to pgque_writer only';
end $$;

do $$
begin
  perform pgque.unregister_consumer('sbx_q', 'c1');
  perform pgque.drop_queue('sbx_q', true);
end $$;
