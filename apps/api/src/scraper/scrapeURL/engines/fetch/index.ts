import { EngineScrapeResult } from "..";
import { Meta } from "../..";
import { TimeoutError } from "../../error";
import { specialtyScrapeCheck } from "../utils/specialtyHandler";

export async function scrapeURLWithFetch(meta: Meta): Promise<EngineScrapeResult> {
    const timeout = 20000;

    const response = await Promise.race([
        fetch(meta.url, {
            redirect: "follow",
            headers: meta.options.headers,
        }),
        (async () => {
            await new Promise((resolve) => setTimeout(() => resolve(null), timeout));
            throw new TimeoutError("Fetch was unable to scrape the page before timing out", { cause: { timeout } });
        })()
    ]);

    specialtyScrapeCheck(meta.logger.child({ method: "scrapeURLWithFetch/specialtyScrapeCheck" }), Object.fromEntries(response.headers as any));

    if (!response.ok) {
        throw new Error(`Fetch was unable to scrape the page ${meta.url}: ${response.statusText}`);
    }

    const contentType = response.headers.get("content-type") || '';
    console.log('contentType', contentType);

    const supportedContentTypes = ["text/html", "text/plain"];

    if (!supportedContentTypes.some(x => contentType.includes(x))) {
        throw new Error(`Fetch was unable to scrape the page ${meta.url}: unsupported content type ${contentType}`);
    }

    return {
        url: response.url,
        html: await response.text(),
        statusCode: response.status,
        // TODO: error?
    };
}
