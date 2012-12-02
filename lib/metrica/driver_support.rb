require 'active_support/concern'
require 'metrica/driver/cassandra'

module Metrica
  module Driver
  end

  module DriverSupport
    extend ::ActiveSupport::Concern

    included do
      def self.driver
        @@driver ||= begin
          case self.storage
          when :cassandra
            Driver::Cassandra.new(self.options)
          when :cassandra_compact
            Driver::CassandraCompact.new(self.options)
          else
            raise "Driver does not supported: #{self.storage}"
          end
        end
      end
    end
  end
end
