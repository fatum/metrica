require 'cassandra/1.1'
require 'metrica/cassandra/counter'
require 'metrica/cassandra/schema'

module Metrica
  module Driver
    class Cassandra
      COLUMN_FAMILY = 'Counters'

      attr_reader :client, :counter, :schema_manager

      delegate :keyspace, to: :client
      delegate :up, :down, to: :schema_manager

      def initialize(options)
        @client = ::Cassandra.new(options[:keyspace], options[:servers])
        @counter = Metrica::Cassandra::Counter.new(@client, COLUMN_FAMILY)
        @schema_manager = Metrica::Cassandra::Schema.new(@client)
      end
    end
  end
end
