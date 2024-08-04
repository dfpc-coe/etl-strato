import { FeatureCollection } from 'geojson';
import { Type, TSchema } from '@sinclair/typebox';
import ETL, { Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';

const Environment = Type.Object({
    URL: Type.String(),
    QueryParams: Type.Optional(Type.Array(Type.Object({
        key: Type.String(),
        value: Type.String()
    }))),
    Headers: Type.Optional(Type.Array(Type.Object({
        key: Type.String(),
        value: Type.String()
    }))),
    RemoveID: Type.Boolean({
        default: false,
        description: 'Remove the provided ID falling back to an Object Hash or Style Override'
    }),
    ShowTrackHistory: Type.Boolean({
        default: true,
        description: 'If true pass through historic track'
    })
});

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Environment
        } else {
            return Type.Object({})
        }
    }

    async control(): Promise<void> {
        const env = await this.env(Environment);

        const url = new URL(env.URL);

        for (const param of env.QueryParams || []) {
            url.searchParams.append(param.key, param.value);
        }

        const headers: Record<string, string> = {};
        for (const header of env.Headers || []) {
            headers[header.key] = header.value;
        }

        const res = await fetch(url, {
            method: 'GET',
            headers
        });

        // TODO: Type the response
        const body: any = await res.json();

        if (body.type !== 'FeatureCollection') {
            throw new Error('Only FeatureCollection is supported');
        }

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: []
        };

        if (body.features.length > 2) {
            throw new Error('API Should only return 2 features');
        }

        for (const feat of body.features) {
            if (env.RemoveID) delete feat.id;

            if (feat.geometry.type === 'Point') {
                feat.geometry.coordinate.push(feat.properties.altitude);

                fc.features.push({
                    id: `strato-${feat.properties.name}-current`,
                    type: 'Feature',
                    properties: {
                        course: feat.properties.course,
                        speed: feat.properties.speed,
                        metadata: feat.properties
                    },
                    geometry: feat.geometry
                });
            } else if (env.ShowTrackHistory) {
                fc.features.push({
                    id: `strato-${feat.properties.name}-history`,
                    type: 'Feature',
                    properties: {
                        metadata: feat.properties
                    },
                    geometry: feat.geometry
                });
            }
        }

        console.log(`ok - obtained ${fc.features.length} features`);

        await this.submit(fc);
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}

