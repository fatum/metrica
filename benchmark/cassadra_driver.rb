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

threads = []

10.times do |i|
  threads << Thread.new(i) do
    1000.times { action.call }
  end
end

threads.each(&:join)
