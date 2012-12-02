require 'metrica/cassandra/counter/handler'

module Metrica
  class Cassandra
    class Counter
      def initialize(client, cf)
        @client, @cf = client, cf
      end

      def counter(metric, by = 1, time = nil)
        time ||= Time.new.to_i
        execute(metric, by, time)
      end

      def histogram(metric, end_time, start_time, type = :minutes)
        handler = case type
        when :minutes
          Handler::Minute.new(metric, [end_time, start_time])
        when :hours
          Handler::Hour.new(metric, [end_time, start_time])
        end

        handler.prepare(@client.get(@cf, handler.row_key))
      end

    private
      module Handler
        class Hour
          attr_reader :metric, :by, :time

          def initialize(metric, time = nil, by = nil)
            @metric, @by, @time = metric, by, time
          end

          def include?(col)
            end_time, start_time = time

            build_column(start_time).to_i <= col.to_i && build_column(end_time).to_i >= col.to_i
          end

          def prepare(metrics)
          end

          def column_key
            build_column(time)
          end

          def row_key
            "#{metric}|#{time.strftime('%Y%m%d')}"
          end

          def build_column(t)
            t.hour
          end
        end

        class Minute < Hour
          attr_reader :metric, :by, :time

          def initialize(metric, time = nil, by = nil)
            @metric, @by, @time = metric, by, time
          end

          def prepare(metrics)
            time_loc = time.respond_to?(:each) ? time.first : time

            h ||= {}
            metrics.each do |row|
              minute, value = row

              next unless include?(minute)

              h[time_loc.dup.change(min: minute)] = value
            end
            h
          end

          def row_key
            time_loc = time.respond_to?(:each) ? time.first : time

            "#{metric}|#{time_loc.strftime('%Y%m%d%H')}"
          end

          def build_column(t)
            if t.respond_to?(:map)
              t.map { |t| t.strftime('%M') }
            else
              t.strftime('%M')
            end
          end
        end
      end

      # ex. user_id:4fg5g, 1, 1454546454
      def execute(metric, by, timestamp)
        time = Time.at(timestamp)

        @client.batch do
          persist Handler::Minute.new(metric, time), by
          persist Handler::Hour.new(metric, time), by
        end
      end

      def persist(handler, by)
        @client.add(@cf, handler.row_key, by, handler.column_key.to_s)
      end
    end
  end
end
