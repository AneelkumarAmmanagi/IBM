const { client } = require("../databaseSetup/elasticsearchConfig");


const getDocsWithWorkspaceAndSummary = async (changeId) => {
    try {
        const result = await client.search({
            index: process.env.ELASTIC_INDEX || 'change-risk-analysis',
            _source: ['number', 'analysis_result', 'workspace', 'dc', 'region', 'service_names'],
            size: 10000,
            query: {
                bool: {
                    must: [
                        { term: { 'number.keyword': changeId } }
                    ]
                }
            },
        });
        return result.hits.hits.map(hit => hit._source);
    } catch (error) {
        console.error('Error fetching data from Elasticsearch:', error);
    }
}

module.exports = {
    getDocsWithWorkspaceAndSummary
}
