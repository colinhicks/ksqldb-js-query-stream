
type Opts = {
    endpoint: string,
    token: string,
    query: string,
}

const readOpts = (form: HTMLElement): Opts => {
    return ['endpoint', 'token', 'query'].reduce((acc, k) => {
        acc[k] = (<HTMLFormElement>form.querySelector(`[name="${k}"]`)).value;
        return acc;
    }, {}) as Opts;
};

const maybeError = (opts: Opts): string?=> {
    const allDefined = Object.values(opts).every(x => x && x.trim());

    if (!allDefined) {
        return 'Fill out the form';
    }

    if (!opts.query.toLowerCase().startsWith('select')) {
        return `Not a transient query: ${opts.query}`;
    }
    return null;
}

const streamAsyncIterator = (stream: ReadableStream) => {
    const reader = stream.getReader();

    return {
        next() {
            return reader.read();
        },
        return() {
            reader.releaseLock();
            return {};
        },
        [Symbol.asyncIterator]() {
            return this;
        }
    };
}

type Header = {
    queryId: string,
    columnNames: string[],
    columnTypes: string[],
};

const streamResponse = async (opts: Opts) => {
    const body = {
        sql: opts.query,
        properties: {
            'auto.offset.reset': 'earliest',
        }
    };

    const ret = await fetch(`${opts.endpoint}/query-stream`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/vnd.ksqlapi.delimited.v1',
            'Authorization': `Bearer ${opts.token}`,
        },
        body: JSON.stringify(body),
        credentials: 'include',
    });

    const decoder = new TextDecoder();
    let header: Header;
    for await (const chunk of streamAsyncIterator(ret.body)) {
        const decoded = decoder.decode(chunk);
        if (!header) {
            header = JSON.parse(decoded);
            console.log('Header', header);
        } else {
            decoded.split('\n').filter(x => x.trim()).forEach((line) => {
                const parsed = JSON.parse(line);
                const row = header.columnNames.reduce((acc, k, i) => (acc[k] = parsed[i], acc), {});
                console.log('Row', row);
            });
        }
    }
};

const main = (form: HTMLElement) => {
    form.addEventListener('submit', (e) => {
        e.preventDefault();
        const opts = readOpts(form);
        const error = maybeError(opts);
        if (error) {
            return console.warn(error);
        }
        streamResponse(opts);
    });
};

main(document.forms[0]);