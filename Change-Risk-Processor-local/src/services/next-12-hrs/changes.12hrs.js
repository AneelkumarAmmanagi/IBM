const { client } = require("../../databaseSetup/elasticsearchConfig");

async function fetchLastNHoursChanges(hours, closedChanges) {
    const indexName = process.env.ELASTIC_INDEX || "change-risk-analysis";
    const size = 500; // batch size per scroll page
    const allDocuments = [];

    if (typeof hours !== "number" || hours <= 0) {
        throw new Error("Please provide a positive number for hours");
    }

    try {
        const gteBound = `now-${hours}h`;
        const lteBound = "now";

        const searchResponse = await client.search({
            index: indexName,
            scroll: "30s",
            size: size,
            sort: ["_doc"],
            query: {
                bool: {
                    must: [
                        {
                            range: {
                                planned_start: {
                                    gte: gteBound,
                                    lte: lteBound,
                                },
                            },
                        },
                        closedChanges
                            ? { term: { "state.keyword": "Closed" } }
                            : { bool: { must_not: [{ term: { "state.keyword": "Closed" } }] } }
                    ],
                },
            },
        });

        let scrollId = searchResponse._scroll_id;
        let documents = searchResponse.hits.hits;
        const total = searchResponse.hits.total.value;

        documents.forEach((doc) => {
            allDocuments.push({
                id: doc._id,
                ...doc._source,
            });
        });

        while (documents.length > 0 && allDocuments.length < total) {
            const scrollResponse = await client.scroll({
                scroll_id: scrollId,
                scroll: "30s",
            });

            scrollId = scrollResponse._scroll_id;
            documents = scrollResponse.hits.hits;

            documents.forEach((doc) => {
                allDocuments.push({
                    id: doc._id,
                    ...doc._source,
                });
            });
        }

        if (scrollId) {
            await client.clearScroll({ scroll_id: scrollId });
        }

        return {
            success: true,
            total,
            documents: allDocuments,
        };
    } catch (error) {
        console.error("Error fetching documents:", error.message);
        throw error;
    }
}


module.exports = {
    fetchLastNHoursChanges,
};