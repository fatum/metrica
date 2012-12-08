module Metrica
  class Cassandra
    module Query
      class Offset
        class Hour
          def initialize(slicer)
            @slicer = slicer
          end

          def fetch(now, offset)
            # from current hour pull past minutes
            current_minute = now.strftime('%M').to_i

            current_hour_minutes(now, current_minute) + last_hour_minutes(offset, current_minute)
          end

          def current_hour_minutes(now, current_minute)
            row_key = @slicer.hour_key(now)
            @slicer.data[row_key].values.map(&:to_i)
          end

          def last_hour_minutes(now, current_minute)
            row_key = @slicer.hour_key(now)
            @slicer.data[row_key].select { |m, v| m.to_i > current_minute }.values.map(&:to_i)
          end
        end

        class Date
        end

        attr_reader :metric, :data

        def initialize(metric, data)
          @metric, @data = metric, data
        end

        def hour_offset(now, offset_date)
          Hour.new(self).fetch(now, offset_date)
        end

        def hour_key(time)
          "#{@metric}|#{time.strftime('%Y%m%d%H')}"
        end
      end
    end
  end
end
