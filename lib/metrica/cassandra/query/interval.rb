module Metrica
  class Cassandra
    module Query
      class Interval
        def initialize(client, cf)
          @client, @cf = client, cf
        end

        def histogram(metric, end_time, start_time, type = :minutes)
          handler = case type
          when :minutes
            Type::Minute.new(metric, [end_time, start_time])
          when :hours
            Type::Hour.new(metric, [end_time, start_time])
          when :days
            Type::Day.new(metric, [end_time, start_time])
          end

          # move prepare into Query::Interval
          handler.prepare(@client.get(@cf, handler.row_key))
        end
      end
    end
  end
end

