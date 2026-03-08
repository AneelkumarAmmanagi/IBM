const { client } = require("../../databaseSetup/elasticsearchConfig");

async function getAllChangeNumbersAndSummariesFromWorkspaceIndex() {
    const sourceIndex = "genctl-release-commit-summary";
    const targetIndex = "change-risk-analysis";

    const changeMap = new Map();
    let totalDocs = 0;
    let totalChangeNumbers = 0;

    try {
        let response = await client.search({
            index: sourceIndex,
            scroll: "2m",
            size: 1000,
            _source: ["repo", "summary", "regions"],
        });

        while (response.hits.hits.length > 0) {
            for (const hit of response.hits.hits) {
                totalDocs++;
                const { repo, summary, regions } = hit._source;
                if (!regions || typeof regions !== "object") continue;

                const changeNumbers = Object.values(regions).flat().filter(Boolean);
                totalChangeNumbers += changeNumbers.length;

                for (const chg of changeNumbers) {
                    if (!chg) continue;
                    if (!changeMap.has(chg)) {
                        changeMap.set(chg, new Map());
                    }

                    const repoMap = changeMap.get(chg);
                    if (!repoMap.has(repo)) {
                        repoMap.set(repo, summary || "");
                    }
                }
            }

            response = await client.scroll({
                scroll_id: response._scroll_id,
                scroll: "2m",
            });
        }

        await client.clearScroll({ scroll_id: response._scroll_id });

        console.log(`✅ Processed ${totalDocs} docs from ${sourceIndex}`);
        console.log(`📦 Total change numbers (including duplicates): ${totalChangeNumbers}`);
        console.log(`🔢 Unique change numbers: ${changeMap.size}`);

        // 2️⃣ Flatten summaries
        const finalMap = new Map();
        for (const [chg, repoMap] of changeMap.entries()) {
            const combinedSummary = Array.from(repoMap.entries())
                .map(([r, s]) => `[${r}] ${s}`)
                .join("\n");
            finalMap.set(chg, combinedSummary);
        }

        console.log(`🧾 Combined summaries generated for ${finalMap.size} change numbers`);

        // 3️⃣ Check if they exist in change-risk-analysis
        const changeNumbers = Array.from(finalMap.keys());
        const batchSize = 1000;
        let wouldUpdate = 0;
        let skippedCount = 0;
        const updates = [];

        for (let i = 0; i < changeNumbers.length; i += batchSize) {
            const batch = changeNumbers.slice(i, i + batchSize);

            const searchRes = await client.search({
                index: targetIndex,
                size: batchSize,
                _source: ["workspace.summary", "number", "dc"],
                query: {
                    terms: { "number.keyword": batch },
                },
            });

            for (const doc of searchRes.hits.hits) {
                const chg = doc._source.number;
                const workspace = doc._source.workspace || {};
                const datacenter = doc._source.dc || "";
                if (!workspace.summary) {
                    wouldUpdate++;
                    console.log(`🟡 Would update CHG: ${chg} → add workspace.summary ${datacenter}`);

                    updates.push(
                        { update: { _index: targetIndex, _id: doc._id } },
                        { doc: { workspace: { ...workspace, summary: finalMap.get(chg)} } }
                    );
                } else {
                    skippedCount++;
                }
            }

            if (updates.length > 0) {
                const bulkRes = await client.bulk({ refresh: true, body: updates });
                if (bulkRes.errors) {
                    console.error("⚠️ Bulk update errors:", bulkRes.errors);
                } else {
                    console.log(`✅ Bulk updated ${updates.length / 2} documents`);
                }
            }

            console.log(`🔍 Batch ${i / batchSize + 1}: found ${searchRes.hits.hits.length} matches`);
        }

        console.log(`✅ Completed dry run`);
        console.log(`📈 Would update: ${wouldUpdate}`);
        console.log(`🚫 Skipped (already had workspace.summary): ${skippedCount}`);

        return { totalDocs, totalChangeNumbers, unique: changeMap.size, wouldUpdate, skippedCount };
    } catch (err) {
        console.error("❌ Error during processing:", err);
        throw err;
    }
}

module.exports = { getAllChangeNumbersAndSummariesFromWorkspaceIndex };
