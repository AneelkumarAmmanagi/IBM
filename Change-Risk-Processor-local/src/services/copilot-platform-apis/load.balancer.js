class ApiLoadBalancer {
    constructor(urls = [], apiKeys = []) {
        this.urls = urls.filter(url => url && url.trim() !== '');
        this.apiKeys = apiKeys.filter(key => key && key.trim() !== '');
        this.currentIndex = 0;
        this.stats = {
            totalRequests: 0,
            successPerUrl: {},
            failuresPerUrl: {},
            lastUsedIndex: -1
        };

        this.urls.forEach((url, index) => {
            this.stats.successPerUrl[index] = 0;
            this.stats.failuresPerUrl[index] = 0;
        });

        console.log(`Load Balancer initialized with ${this.urls.length} URL(s)`);
        if (this.urls.length === 0) {
            console.warn('WARNING: No URLs configured for load balancer!');
        }
    }

    getNextEndpoint() {
        if (this.urls.length === 0) {
            throw new Error('No URLs configured for load balancer');
        }

        const index = this.currentIndex % this.urls.length;
        const url = this.urls[index];
        const apiKey = this.apiKeys[index] || this.apiKeys[0] || '';

        this.currentIndex = (this.currentIndex + 1) % this.urls.length;
        this.stats.lastUsedIndex = index;
        this.stats.totalRequests++;

        return { url, apiKey, index };
    }

    recordSuccess(index) {
        if (this.stats.successPerUrl.hasOwnProperty(index)) {
            this.stats.successPerUrl[index]++;
        }
    }

    recordFailure(index) {
        if (this.stats.failuresPerUrl.hasOwnProperty(index)) {
            this.stats.failuresPerUrl[index]++;
        }
    }

    getStats() {
        return {
            totalUrls: this.urls.length,
            totalRequests: this.stats.totalRequests,
            successPerUrl: { ...this.stats.successPerUrl },
            failuresPerUrl: { ...this.stats.failuresPerUrl },
            urls: this.urls.map((url, i) => ({
                url: url,
                success: this.stats.successPerUrl[i] || 0,
                failures: this.stats.failuresPerUrl[i] || 0,
                successRate: this.calculateSuccessRate(i)
            }))
        };
    }

    calculateSuccessRate(index) {
        const success = this.stats.successPerUrl[index] || 0;
        const failures = this.stats.failuresPerUrl[index] || 0;
        const total = success + failures;

        if (total === 0) return '0.00%';
        return ((success / total) * 100).toFixed(2) + '%';
    }

    resetStats() {
        this.stats.totalRequests = 0;
        this.urls.forEach((url, index) => {
            this.stats.successPerUrl[index] = 0;
            this.stats.failuresPerUrl[index] = 0;
        });
        console.log('Load balancer statistics reset');
    }

    getUrlCount() {
        return this.urls.length;
    }

    isConfigured() {
        return this.urls.length > 0;
    }
}

function initializeLoadBalancer() {
    const urls = [];
    const apiKeys = [];

    const multipleUrls = process.env.COPILOT_PLATFORM_CHANGE_ANALYSIS_URLS;
    if (multipleUrls) {
        const urlList = multipleUrls.split(',').map(url => url.trim()).filter(url => url);
        urls.push(...urlList);
        console.log(`Loaded ${urlList.length} URLs from COPILOT_PLATFORM_CHANGE_ANALYSIS_URLS`);
    }

    const singleUrlEnv = process.env.COPILOT_PLATFORM_CHANGE_ANALYSIS_URL;
    if (singleUrlEnv) {
        if (singleUrlEnv.includes(',')) {
            const urlList = singleUrlEnv.split(',').map(url => url.trim()).filter(url => url);
            const newUrls = urlList.filter(url => !urls.includes(url));
            if (newUrls.length > 0) {
                urls.push(...newUrls);
                console.log(`Loaded ${newUrls.length} URLs from COPILOT_PLATFORM_CHANGE_ANALYSIS_URL (comma-separated detected)`);
            }
        } else {
            if (!urls.includes(singleUrlEnv)) {
                urls.push(singleUrlEnv);
                console.log('Loaded 1 URL from COPILOT_PLATFORM_CHANGE_ANALYSIS_URL');
            }
        }
    }

    const multipleKeys = process.env.COPILOT_PLATFORM_CHANGE_ANALYSIS_API_KEYS;
    if (multipleKeys) {
        const keyList = multipleKeys.split(',').map(key => key.trim()).filter(key => key);
        apiKeys.push(...keyList);
    }

    const singleKey = process.env.COPILOT_PLATFORM_CHANGE_ANALYSIS_API_KEY;
    if (singleKey && !apiKeys.includes(singleKey)) {
        apiKeys.push(singleKey);
    }

    const balancer = new ApiLoadBalancer(urls, apiKeys);

    const throughputPerMin = balancer.getUrlCount() * 100;

    console.log('\n=== Load Balancer Configuration ===');
    console.log(`Total URLs configured: ${balancer.getUrlCount()}`);
    console.log(`Throughput capacity: ${throughputPerMin} requests/min`);
    console.log(`Throughput capacity: ${throughputPerMin * 60} requests/hour`);
    console.log('\nEstimated processing times:');
    console.log(`  - 100 docs: ~${Math.ceil(100 / throughputPerMin)} min`);
    console.log(`  - 500 docs: ~${Math.ceil(500 / throughputPerMin)} min`);
    console.log(`  - 1000 docs: ~${Math.ceil(1000 / throughputPerMin)} min`);
    console.log(`  - 5000 docs: ~${Math.ceil(5000 / throughputPerMin)} min`);
    console.log('===================================\n');

    return balancer;
}

let loadBalancerInstance = null;


function getLoadBalancer() {
    if (!loadBalancerInstance) {
        loadBalancerInstance = initializeLoadBalancer();
    }
    return loadBalancerInstance;
}

module.exports = {
    ApiLoadBalancer,
    getLoadBalancer,
    initializeLoadBalancer
};

