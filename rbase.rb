require 'date'

class Table < Array
  attr :name, :attributes, :index

  def initialize(name, attributes, index = -1)
    @name = name
    @attributes = attributes
    @index = index # self-incrementing serial number for id
  end

  def select_all(attribute, op, val, type='string')
    case type
      when 'integer' then to_type = lambda { |v| v.to_i }
      when 'time' then to_type = lambda { |v| DateTime.parse(v) }
      else to_type = lambda { |v| v }
    end
    value = to_type.call(val)

    case op
      when 'is'   then self.compact.keep_if {|row| row[attribute] == val}
      when 'not'  then self.compact.keep_if {|row| row[attribute] != val}
      when 'gt'   then self.compact.keep_if {|row| to_type.call(row[attribute]) > value}
      when 'lt'   then self.compact.keep_if {|row| to_type.call(row[attribute]) < value}
      when 'like' then self.compact.keep_if {|row| row[attribute].include?(val)}
    end
  end

  def insert(row)
    @index += 1
    self[@index] = row.merge("id" => @index.to_s)
    @index
  end

  def destroy(index)
    self[index.to_i] = nil
  end

  def get(index)
    self[index.to_i]
  end

  def persist(f)
    f.puts({name: name, attributes: attributes, index: index}.to.json)
    f.puts(self.count.to_s)
    self.each do |row|
      f.puts(row.to_json)
    end
  end
end

configure do
  set :database, {}
  # load persisted data on startup
  File.open("./rbase.index", "a+") do |f|
    tables_data = f.readline rescue []
    unless tables_data.empty?
      tables = JSON.parse tables_data
      tables.each do |table_name|
        table_head = JSON.parse f.readline
        count = f.readline.to_i
        table = Table.new(table_head['name'], table_head['attributes'], table_head['index'])
        settings.database[table_name] = table
        count.times.each do |i|
          table[i] = JSON.parse(f.readline)
        end
      end
    end
  end
end

# convenience method to prepare the table
def prepare_table(table_name)
  raise 'No such table' unless (table = settings.database[table_name])
  table
end

# show the schema
get "/schema" do
  list = {}
  settings.database.each do |schema, table|
    list[schema] = table.attributes
  end
  list.to_json
end

# set up schema
# clears existing schema and database if give ?clear=true
post "/schema" do
  settings.database.clear if params[:clear]
  schema = JSON.parse params[:schema]
  raise 'Schema not provided or wrong schema' unless schema
  schema.each do |tablename, attributes|
    unless settings.database.keys.include?(tablename)
      settings.database[tablename] = Table.new(tablename, attributes)
    end
  end
  [200, 'Schema Created'] # probably need to handle case schema already exist
end

# insert
post "/:table" do
  table = prepare_table(params[:table])
  row_data = JSON.parse params[:row]
  raise 'No insert data provided' unless row_data
  index = table.insert(row_data)
  [200, index.to_s]
end

# update
put "/:table/:id" do
  table = prepare_table(params[:table])
  row = table.get(params[:id])
  row_data = JSON.parse params[:row]
  raise 'No insert data provided' unless row_data

  row.merge!(row_data)
  [200, row['id'].to_s ]
end

# get a single row
get "/:table/:id" do
  table = prepare_table(params[:table])
  row = table.get(params[:id])
  raise "Row not found" unless row
  row.to_json
end

# select
get "/:table/:attribute/:op/:value" do
  table = prepare_table(params[:table])
  type = params[:type] || 'string'
  selection = table.select_all(params[:attribute], params[:op], params[:value], type)
  selection.to_json
end

# delete
delete "/:table/:id" do
  table = prepare_table(params[:table])
  table.destroy(params[:id])
  [200]
end

# persist to file
get "/persist" do
  File.open("./rbase.index", "w") do |f|
    f.puts(settings.database.keys)
    settings.database.each do |name, table|
      table.persist(f)
    end
  end
  [200]
end
