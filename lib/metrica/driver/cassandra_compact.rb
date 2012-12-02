require 'cassandra/1.1'

module Metrica
  module Driver
    class CassandraCompact
      attr_reader :client, :counter, :schema_manager

      def initialize(options)
        @client = ::Cassandra.new(options[:keyspace], options[:servers])
        @counter = Metrica::CassandraCompact::Counter.new(@client, COLUMN_FAMILY)
      end
    end
  end
end
