# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"

class TestProcessed < Minitest::Test
  include PgqueTest::Helpers

  def test_processed_once_the_batch_is_acked
    with_queue do |queue, consumer, conn|
      client = Pgque::Client.new(conn)
      txid = conn.transaction do
        client.send(queue, { "a" => 1 })
        conn.exec("select pg_current_xact_id()").getvalue(0, 0)
      end
      refute client.processed?(queue, consumer, txid)

      conn.exec_params("select pgque.force_next_tick($1)", [queue])
      conn.exec_params("select pgque.ticker($1)", [queue])
      msgs = client.receive(queue, consumer, 10)
      refute client.processed?(queue, consumer, txid)

      client.ack(msgs[0].batch_id)
      assert client.processed?(queue, consumer, txid)
    end
  end

  def test_processed_raises_for_an_unknown_consumer
    with_queue do |queue, _consumer, conn|
      assert_raises(Pgque::Error) do
        Pgque::Client.new(conn).processed?(queue, "nobody", 1)
      end
    end
  end
end
