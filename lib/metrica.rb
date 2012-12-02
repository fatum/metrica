require "active_support/core_ext"

require "metrica/version"
require 'metrica/api'
require 'metrica/driver_support'

module Metrica
  class << self
    attr_accessor :storage, :options
  end

  def self.configure(&block)
    yield(self)
  end

  include Api
  include DriverSupport
end
