# encoding: UTF-8
require "rubygems"
base = File.dirname(__FILE__)
$:.unshift File.expand_path(File.join(base, "..", "lib"))

require "irb"
require "yaml"
require "messagebus"

body = 
 
{
    "source_name"=>"google_places",
    "name"=>"Goodman Distribution",
    "location"=>
      {
        "address"=>"12552 Texas 3",
        "address_extended"=>nil,
        "locality"=>"Webster",
        "postcode"=>"77598",
        "region"=>"TX",
        "country"=>"US",
        "lon_lat"=>{
          "lon"=>-95.167307,
          "lat"=>29.58916}
          },
    "phone_numbers" =>["281 480 5100"],
    "website"=>"http://www.goodmanmfg.com/",
    "hours"=>['Monday 7:30 am – 5:00 pm,Tuesday 7:30 am – 5:00 pm,Wednesday 7:30 am – 5:00 pm,Thursday 7:30 am – 5:00 pm,Friday 7:30 am – 5:00 pm,Saturday Closed,Sunday Closed'],
    #"hours"=>['Thorbjørn'],
    "status"=>nil,
    "categories"=>nil,
    "external_references"=>
      {
        "google_places"=>
          {
            "external_access_timestamp"=>"2012-07-20T20:18:22+0000",
            "content"=>
              {
                "source_categories"=>["Construction","Air Conditioning Distribution","Air Conditioning Contractor","HVAC Distribution"]
              },
            "external_url"=>"http://maps.google.com/maps/place?cid=",
            "external_id"=>nil
          }
      },
      "menus"=>nil,
      "email_contact"=>nil,"chain_name"=>nil,
      "year_established"=>nil
} 
def nullify_hash(hash)
  if (hash.is_a? Hash)
    hash.keys.each do |key|
      hash[key] = nullify_hash(hash[key])
    end
  else
    return nil if hash == ""
  end
  hash
end
body = nullify_hash(body)

config = YAML.load_file("../config/messagebus.yml")
client = Messagebus::Client.new(config.merge(:logger => Logger.new("../mbus.log")))
client.start
success=client.publish("jms.topic.grouponTestTopic2", body)
puts success
client.stop
=begin
 {
      "source_name" => m3_entry["sources"][0]["source_name"],
      "name" => m3_entry["name"],
      "location" => {
          "address" => m3_entry["location"]["street_address"],
          "address_extended" => nil,
          "locality" => m3_entry["location"]["city"],
          "postcode" => m3_entry["location"]["postal_code"],
          "region" => m3_entry["location"]["state"],
          "country" => m3_entry["location"]["country"],
          "lon_lat" => m3_entry["location"]["lon_lat"],
      },
      "phone_numbers" => [m3_entry["phone_number"]],
      "website" => m3_entry["website"],
      "hours" => [m3_entry["hours_open"]],
      "status" => m3_entry["status"],
      "categories" => m3_entry["categories"],
      "external_references" => {m3_entry["sources"][0]["source_name"] => external_ref},
      "menus" => m3_entry["external_references"][0]["menus"],
      "email_contact" => m3_entry["email_contact"],
      "chain_name" => m3_entry["chain_name"],
      "year_established" => m3_entry["year_established"],
  }
=end  
