require 'messagebus/dottable_hash'

describe Messagebus::DottableHash do
  let (:dottable_hash) { Messagebus::DottableHash.new }

  it "is a wrapper of Hash" do
    Messagebus::DottableHash.ancestors.should include(Hash)
  end

  it "optionally takes a hash in the constructor" do
    Messagebus::DottableHash.new({:this => "works"})
  end

  it "allows dot assignment" do
    dottable_hash.foo = "bar"
    dottable_hash.should == {"foo" => "bar"}
  end

  it "allows dot access" do
    dottable_hash.foo = "baz"
    dottable_hash.foo.should == "baz"
  end

  it "allows bracket assignment with a String key" do
    dottable_hash["merica"] = "the free"
    dottable_hash.should == {"merica" => "the free"}
  end

  it "allows bracket access with a String key" do
    dottable_hash["foo"] = "hey"
    dottable_hash["foo"].should == "hey"
  end

  it "allows bracket assignment with a Symbol key" do
    dottable_hash[:patrick] = "gombert"
    dottable_hash.should == {"patrick" => "gombert"}
  end

  it "allows bracket access with a Symbol key" do
    dottable_hash[:patrick] = "gombert"
    dottable_hash[:patrick].should == "gombert"
  end

  it "converts keys for Hashes upon bracket assignment" do
    dottable_hash[:dave] = {:surname => {:original => "moore"}}
    dottable_hash.dave.surname.original.should == "moore"
  end

  it "remains dottable after merging a plain ruby Hash" do
    dottable_hash.merge!({:some => "plain old hash"})
    dottable_hash.some.should == "plain old hash"
  end

  it "converts all keys in nested Hashes of any depth" do
    dottable_hash.merge!({:some => {:plain => {:old => "hash"}}})
    dottable_hash.should == {"some" => {"plain" => {"old" => "hash"}}}
  end

  it "converts all keys nested in an Array to Strings" do
    dottable_hash.merge!({:some => [{:plain => "old hash"}]})
    dottable_hash.should == {"some" => [{"plain" => "old hash"}]}
  end

  it "converts all keys in nested Arrays of any depth" do
    dottable_hash.merge!({:some => [[{:plain => "old hash"}], "foo"]})
    dottable_hash.should == {"some" => [[{"plain" => "old hash"}], "foo"]}
  end

  it "ignores elements which are not Hashes or Arrays" do
    dottable_hash.merge!({:some => [{:plain => "old hash"}, "foo"]})
    dottable_hash.should == {"some" => [{"plain" => "old hash"}, "foo"]}
  end

  it "makes deeply-nested hash structures dottable upon initialization" do
    dottable_hash = Messagebus::DottableHash.new({:some => {:plain => {:old => "hash"}}})
    dottable_hash.some.plain.old.should == "hash"
  end

  it "makes deeply Array-nested Hash structures dottable upon initialization" do
    dottable_hash = Messagebus::DottableHash.new({:some => [[{:plain => "old hash"}]]})
    dottable_hash.some[0][0].plain.should == "old hash"
  end

  it "#replace converts a plain old ruby hash to a dottable hash" do
    replacement = {:dave => {:moore => "awesome"}}
    result = Messagebus::DottableHash.new({ "foo" => "bar" }).replace(replacement)
    result.dave.moore.should == "awesome"
    result.has_key?("foo").should == false
  end

  it "#update converts a plain old ruby hash to a dottable hash" do
    result = Messagebus::DottableHash.new({ "foo" => "bar" }).update({:something => :else})
    result.should == {"something" => :else, "foo" => "bar"}
  end

  it "#merge converts a plain old ruby hash to a dottable hash" do
    result = Messagebus::DottableHash.new({ "foo" => "bar" }).merge({:something => :else})
    result.should == {"something" => :else, "foo" => "bar"}
  end

  it "#delete accepts Symbol as an argument" do
    dottable_hash["foo"] = { "bar" => "bar", "baz" => "baz" }
    dottable_hash.foo.delete(:bar)
    dottable_hash.should == { "foo" => { "baz" => "baz" } }
  end

  it "#has_key? accepts Symbol as an argument" do
    Messagebus::DottableHash.new({ "foo" => "bar" }).has_key?(:foo).should == true
  end

  it "#key? accepts a Symbol as an argument" do
    Messagebus::DottableHash.new({ "foo" => "bar" }).key?(:foo).should == true
  end

  it "#fetch accepts a Symbol as an argument" do
    Messagebus::DottableHash.new({ "foo" => "bar" }).fetch(:foo).should == "bar"
  end

  it "#include? accepts a Symbol as an argument" do
    Messagebus::DottableHash.new({ "foo" => "bar" }).include?(:foo).should == true
  end

  it "#store accepts a Symbol as an argument" do
    hash = Messagebus::DottableHash.new({ "foo" => "bar" })
    hash.store(:bears, "scare me")
    hash.bears.should == "scare me"
  end

  it "#values_at accepts Symbols as arguments" do
    dottable_hash.merge({ "foo" => "bar", "zen" => "cool", "hey" => "now" })
    dottable_hash.values_at(:foo, :zen).should =~ ["bar", "cool"]
  end

  it "#respond_to? returns true for everything" do
    dottable_hash.respond_to?(:whatever).should == true
  end
end
