# encoding; utf-8

require 'java'

java_import org.elasticsearch.node.NodeBuilder
java_import org.elasticsearch.action.search.SearchResponse
java_import org.elasticsearch.action.search.SearchType
java_import org.elasticsearch.index.query.QueryBuilders
java_import org.elasticsearch.index.query.QueryStringQueryBuilder

require 'sinatra/base'
require 'json'

#node = NodeBuilder.nodeBuilder().local(true).node();
node = NodeBuilder.nodeBuilder().node();
$client = node.client();

class EsAPI < Sinatra::Base
  get '/search' do
    query = @params['query']
    fields = ! @params['fields'] ? ["_all"] : @params['fields'].split(/,/)
    from = @params['from'] ? @params['from'].to_i : 0
    size = @params['size'] ? @params['size'].to_i : 10

    qsq = QueryBuilders.queryStringQuery(query)
    fields.each do | f |
      qsq.field(f)
    end
    qsq.defaultOperator(QueryStringQueryBuilder::Operator::AND)

    response = $client.prepareSearch(@params['index'])
                      .setTypes(@params['type'])
                      .setSearchType(SearchType::DFS_QUERY_THEN_FETCH)
                      .setQuery(qsq)
                      .setFrom(from).setSize(size)
                      .execute()
                      .actionGet()
    response.to_s
  end
  run!
end


