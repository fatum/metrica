require 'metrica/cassandra/type'
require 'metrica/cassandra/query/offset'
require 'metrica/cassandra/query/interval'

module Metrica
  class Cassandra
    class Counter
      def initialize(client, cf)
        @client, @cf = client, cf
      end

      def increment(metric, by = 1, time = nil)
        time ||= Time.new.to_i
        execute(metric, by, time)
      end

      # ex. user_id:4fg5g, 1, 1454546454
      def execute(metric, by, timestamp)
        time = Time.at(timestamp)

        @client.batch do
          persist Type::Minute.new(metric, time), by
          persist Type::Hour.new(metric, time), by
          persist Type::Day.new(metric, time), by
          persist Type::Month.new(metric, time), by
        end
      end

      def persist(handler, by)
        @client.add(@cf, handler.row_key, by, handler.column_key.to_s)
      end
    end
  end
end
