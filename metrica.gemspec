# -*- encoding: utf-8 -*-
require File.expand_path('../lib/metrica/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["m.filippovich"]
  gem.email         = ["m.filippovich@fun-box.ru"]
  gem.description   = %q{TODO: Write a gem description}
  gem.summary       = %q{TODO: Write a gem summary}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "metrica"
  gem.require_paths = ["lib"]
  gem.version       = Metrica::VERSION

  gem.add_dependency 'cassandra'
  gem.add_dependency 'thrift_client', '0.8.2'
  gem.add_dependency 'activesupport', '3.2.9'

  gem.add_development_dependency 'rspec'
  gem.add_development_dependency 'pry'
end
