module Metrica
  class Cassandra
    module Query
      class Offset
        class Hour
          def initialize(slicer)
            @slicer = slicer
          end

          def fetch(now)
            @data = get_data(now)

            offset = now - 1.hour

            # from current hour pull past minutes
            current_minute = now.strftime('%M').to_i

            offset_values = current_hour_minutes(now, current_minute) + last_hour_minutes(offset, current_minute)
            offset_values.inject(&:+) || 0
          end

          def current_hour_minutes(now, current_minute)
            row_key = @slicer.hour_key(now)
            if @data[row_key]
              @data[row_key].values.map(&:to_i)
            else
              []
            end
          end

          def last_hour_minutes(now, current_minute)
            row_key = @slicer.hour_key(now)
            @data[row_key].select { |m, v| m.to_i > current_minute }.values.map(&:to_i)
          end

          def get_data(time)
            @data ||= RowLoader.new(@slicer).for_hour(time)
          end
        end

        class Minute
          def initialize(slicer)
            @slicer = slicer
          end

          def fetch(offset)
            offset_minute = offset.strftime('%M').to_i
            row_key = @slicer.hour_key(offset)

            current_hour_row = get_data(offset)[row_key].find { |m, v| m.to_i == offset_minute }
            if current_hour_row
              current_hour_row.last.to_i
            else
              0
            end
          end

          def get_data(offset)
            @data ||= RowLoader.new(@slicer).for_minute(offset)
          end
        end

        class Day
          def initialize(slicer)
            @slicer = slicer
          end

          def fetch(offset)
            @data = get_data(offset)

            current_minute = offset.strftime('%M').to_i
            current_hour = offset.hour

            #current day
            key = @slicer.day_key(offset)
            hour_key = @slicer.hour_key(offset)

            # pull all hours from day row less than current hour
            day_value = if @data[key]
              @data[key].select { |h, v| h.to_i < current_hour }.values.map(&:to_i).inject(&:+) || 0
            else
              0
            end

            # pull all minutes from hour row less than current minute
            hour_value = if @data[hour_key]
              @data[hour_key].select { |m, v| m.to_i <= current_minute }.values.map(&:to_i).inject(&:+) || 0
            else
              0
            end

            # day ago
            key = @slicer.day_key(offset - 1.day)
            hour_key = @slicer.hour_key(offset - 1.day)

            offset_day_value = if @data[key]
              @data[key].select { |h, v| h.to_i > current_hour }.values.map(&:to_i).inject(&:+) || 0
            else
             0
            end

            offset_hour_value = if @data[hour_key]
              @data[hour_key].select { |m, v| m.to_i >= current_minute }.values.map(&:to_i).inject(&:+) || 0
            else
              0
            end

            day_value + offset_day_value + hour_value + offset_hour_value
          end

          def get_data(time)
            @data ||= RowLoader.new(@slicer).for_day(time)
          end
        end

        class RowLoader
          def initialize(slicer)
            @slicer, @client, @cf = slicer, Metrica.driver.client, 'Counters'
          end

          def for_hour(time)
            now = time - 1.hour
            keys = [@slicer.hour_key(now), @slicer.hour_key(time)]

            @client.multi_get(@cf, keys)
          end

          def for_minute(time)
            keys = [@slicer.hour_key(time)]
            @client.multi_get(@cf, keys)
          end

          def for_day(time)
            keys = []
            keys << @slicer.day_key(time)
            keys << @slicer.hour_key(time)

            keys << @slicer.day_key(time - 1.day)
            keys << @slicer.hour_key(time - 1.day)

            @client.multi_get(@cf, keys)
          end
        end

        attr_reader :metric, :data

        def initialize(metric)
          @metric = metric
        end

        def minute(offset)
          Minute.new(self).fetch(offset)
        end

        def hour(offset)
          Hour.new(self).fetch(offset)
        end

        def day(offset)
          Day.new(self).fetch(offset)
        end

        def hour_key(time)
          "#{@metric}|#{time.strftime('%Y%m%d%H')}"
        end

        def day_key(time)
          "#{@metric}|#{time.strftime('%Y%m%d')}"
        end
      end
    end
  end
end
