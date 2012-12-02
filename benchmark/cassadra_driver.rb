require 'rubygems'
require 'bundler/setup'
Bundler.require

require 'metrica'
require 'benchmark'

Metrica.configure do |config|
  config.storage = :cassandra
  config.options = { keyspace: 'test', servers: '5.9.90.15:9160' }
end

action = -> {
  begin
    Metrica.driver.counter.increment('some metric')
  rescue CassandraThrift::Cassandra::Client::TransportException => e
    puts "Failed: #{e.message}"
  end
}

time = Benchmark.realtime do
  (1..1000).each { action.call }
end
puts "Time elapsed #{time*1000} milliseconds"
