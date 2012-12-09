module Metrica
  class Cassandra
    module Type
      module Base
        extend ActiveSupport::Concern

        included do
          attr_reader :metric, :by, :time

          def initialize(metric, time = nil, by = nil)
            @metric, @by, @time = metric, by, time
          end

          def prepare(metrics)
            time_loc = time.respond_to?(:each) ? time.first : time

            h ||= {}
            metrics.each do |row|
              time_type, value = row

              next unless include?(time_type)

              params = {type => time_type}
              h[time_loc.dup.change(params)] = value
            end
            h
          end

          def include?(col)
            end_time, start_time = time

            started?(start_time, col) && ended?(end_time, col)
          end

          def started?(start_time, col)
            build_column(start_time).to_i <= col.to_i
          end

          def ended?(end_time, col)
            build_column(end_time).to_i >= col.to_i
          end

          def column_key
            build_column(time)
          end
        end
      end

      class Month
        include Base

        # rename to row
        def row_key
          time_loc = time.respond_to?(:each) ? time.first : time

          "#{metric}|#{time_loc.strftime('%Y')}"
        end

        # rename to column
        def build_column(t)
          t.month
        end

        def type
          :month
        end
      end

      class Day
        include Base

        def row_key
          time_loc = time.respond_to?(:each) ? time.first : time

          "#{metric}|#{time_loc.strftime('%Y%m')}"
        end

        def build_column(t)
          t.day
        end

        def type
          :day
        end
      end

      class Hour
        include Base

        def row_key
          time_loc = time.respond_to?(:each) ? time.first : time

          "#{metric}|#{time_loc.strftime('%Y%m%d')}"
        end

        def build_column(t)
          t.hour
        end

        def type
          :hour
        end
      end

      class Minute
        include Base

        def row_key
          time_loc = time.respond_to?(:each) ? time.first : time

          "#{metric}|#{time_loc.strftime('%Y%m%d%H')}"
        end

        def type
          :min
        end

        def build_column(t)
          t.strftime('%M')
        end
      end
    end
  end
end
