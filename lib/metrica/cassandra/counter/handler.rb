module Metrica
  class Cassandra
    class Counter
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
            time_loc = time.respond_to?(:each) ? time.first : time

            h ||= {}
            metrics.each do |row|
              hour, value = row

              next unless include?(hour)

              h[time_loc.dup.change(hour: hour)] = value
            end
            h
          end

          def column_key
            build_column(time)
          end

          def row_key
            time_loc = time.respond_to?(:each) ? time.first : time

            "#{metric}|#{time_loc.strftime('%Y%m%d')}"
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
    end
  end
end
