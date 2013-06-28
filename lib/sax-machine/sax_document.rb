require "nokogiri"

module SAXMachine
  def self.included(base)
    base.extend ClassMethods
  end

  def parse(xml)
    sax_handler = SAXHandler.new(self)
    parser = Nokogiri::XML::SAX::Parser.new(sax_handler)

    Thread.new do
      begin
        parser.parse(xml)
      rescue => ex
        lazy_queue << {exception: ex}
      ensure
        lazy_queue << nil
      end
    end

    self
  end

  def parse_file(filename)
    sax_handler = SAXHandler.new(self)
    parser = Nokogiri::XML::SAX::Parser.new(sax_handler)

    Thread.new do
      begin
        parser.parse_file(filename)
      rescue => ex
        lazy_queue << {exception: ex}
      ensure
        lazy_queue << nil
      end
    end

    self
  end

  def lazy_queue
   @lazy_queue ||= SizedQueue.new(1)
  end

  module ClassMethods

    def parse(*args)
      new.parse(*args)
    end

    def parse_file(*args)
      new.parse_file(*args)
    end

    def element(name, options = {})
      options[:as] ||= name
      sax_config.add_top_level_element(name, options)

      # we only want to insert the getter and setter if they haven't defined it from elsewhere.
      # this is how we allow custom parsing behavior. So you could define the setter
      # and have it parse the string into a date or whatever.
      method_names = instance_methods.collect(&:to_s)
      attr_reader options[:as] unless method_names.include?(options[:as].to_s)
      attr_writer options[:as] unless method_names.include?("#{options[:as]}=".to_s)
    end

    def attribute(name, options = {})
      options[:as] ||= name
      sax_config.add_top_level_attribute(self.class.to_s, options.merge(:name => name))

      attr_reader options[:as] unless instance_methods.include?(options[:as].to_s)
      attr_writer options[:as] unless instance_methods.include?("#{options[:as]}=")
    end

    def value(name, options = {})
      options[:as] ||= name
      sax_config.add_top_level_element_value(self.class.to_s, options.merge(:name => name))

      attr_reader options[:as] unless instance_methods.include?(options[:as].to_s)
      attr_writer options[:as] unless instance_methods.include?("#{options[:as]}=")
    end

    def columns
      sax_config.columns
    end

    def column(sym)
      columns.select{|c| c.column == sym}[0]
    end

    def data_class(sym)
      column(sym).data_class
    end

    def required?(sym)
      column(sym).required?
    end

    def column_names
      columns.map{|e| e.column}
    end

    def elements(name, options = {})
      options[:as] ||= name
      if options[:class]
        sax_config.add_collection_element(name, options)
      else
        sax_config.add_top_level_element(name, options.merge(:collection => true))
      end

      if options[:lazy]
        class_eval <<-SRC
          def add_#{options[:as]}(value)
            lazy_queue << {value: value}
          end
        SRC
      else
        class_eval <<-SRC
          def add_#{options[:as]}(value)
            #{options[:as]} << value
          end
        SRC
      end

      if options[:lazy]
        class_eval <<-SRC
          def #{options[:as]}
            @#{options[:as]} ||= Enumerator.new do |yielderr|
              while r = lazy_queue.pop
                raise r[:exception] if r[:exception]
                yielderr << r[:value]
              end
            end
          end
        SRC
      else
        class_eval <<-SRC if !instance_methods.include?(options[:as].to_s)
          def #{options[:as]}
            @#{options[:as]} ||= []
          end
        SRC
      end

      attr_writer options[:as] unless instance_methods.include?("#{options[:as]}=".to_sym)
    end

    def sax_config
      @sax_config ||= SAXConfig.new
    end
  end

end
