module Metrica
  class Cassandra
    class Schema
      def initialize(client)
        @client = client
      end

      def up
      end

      def down
      end
    end
  end
end
