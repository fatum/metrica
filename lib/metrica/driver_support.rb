require 'active_support/concern'

module Metrica
  module Driver
    autoload :Cassandra, 'metrica/driver/cassandra'
    autoload :CassandraCompact, 'metrica/driver/cassandra_compact'
    autoload :Vertica, 'metrica/driver/vertica'
  end

  module DriverSupport
    extend ::ActiveSupport::Concern

    included do
      def self.reconnect!
        Thread.current['driver'] = nil
      end

      def self.driver
        Thread.current['driver'] ||= begin
          case self.storage
          when :cassandra
            Driver::Cassandra.new(self.options)
          when :cassandra_compact
            Driver::CassandraCompact.new(self.options)
          when :vertica
            Driver::Vertica.new(self.options)
          else
            raise "Driver does not supported: #{self.storage}"
          end
        end
      end
    end
  end
end
