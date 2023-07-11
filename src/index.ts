import {
  mapSchema,
  getDirectives,
  MapperKind,
  printSchemaWithDirectives,
} from '@graphql-tools/utils'
import {
  wrapSchema,
  TransformObjectFields,
  TransformInterfaceFields,
} from '@graphql-tools/wrap'
import {
  GraphQLSchema,
  GraphQLNamedType,
  GraphQLOutputType,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLInputObjectType,
  GraphQLFieldConfig,
  GraphQLFieldConfigArgumentMap,
  GraphQLList,
  GraphQLScalarType,
  GraphQLSchemaConfig,
  GraphQLObjectTypeConfig,
  GraphQLInterfaceTypeConfig,
  GraphQLUnionTypeConfig,
  GraphQLScalarTypeConfig,
  GraphQLEnumTypeConfig,
  GraphQLEnumValue,
  GraphQLEnumValueConfig,
  GraphQLInputObjectTypeConfig,
  GraphQLField,
  GraphQLInputField,
  GraphQLInputFieldConfig,
} from 'graphql'
import { makeExecutableSchema, mergeSchemas } from '@graphql-tools/schema'

type DirectableGraphQLObject =
  | GraphQLSchema
  | GraphQLSchemaConfig
  | GraphQLNamedType
  | GraphQLObjectTypeConfig<any, any>
  | GraphQLInterfaceTypeConfig<any, any>
  | GraphQLUnionTypeConfig<any, any>
  | GraphQLScalarTypeConfig<any, any>
  | GraphQLEnumTypeConfig
  | GraphQLEnumValue
  | GraphQLEnumValueConfig
  | GraphQLInputObjectTypeConfig
  | GraphQLField<any, any>
  | GraphQLInputField
  | GraphQLFieldConfig<any, any>
  | GraphQLInputFieldConfig

export type PaginationDirectiveOptions = {
  useCacheControl?: boolean
}

export default function paginationDirective(
  directiveName: string,
  options?: PaginationDirectiveOptions
) {
  return {
    paginationDirectiveTypeDefs: `directive @${directiveName} on FIELD_DEFINITION`,
    paginationDirectiveTransform: (schema: GraphQLSchema) => {
      const newTypeDefs: string[] = []
      const foundTypes: { [name: string]: GraphQLNamedType } = {}
      const paginationTypes: { [name: string]: boolean } = {}
      const markedLocations: { [name: string]: string } = {}

      // variables for cacheControl:
      const paginationTypeGreatestMaxAge: {
        [returnTypeName: string]: number
      } = {}

      function handleCacheControlDirectiveVisitation(
        node: DirectableGraphQLObject,
        typeName: string
      ) {
        const directives = getDirectives(schema, node)
        const cacheControlDirective = directives?.find(
          (d) => d.name === 'cacheControl'
        )
        if (cacheControlDirective) {
          const maxAge = cacheControlDirective.args?.maxAge

          if (typeof maxAge === 'number') {
            const baseName = getBaseType(typeName)
            if (
              !paginationTypeGreatestMaxAge.hasOwnProperty(baseName) ||
              maxAge > paginationTypeGreatestMaxAge[baseName]
            ) {
              paginationTypeGreatestMaxAge[baseName] = maxAge
            }
          }
        }
      }

      // Perform visitations:
      const fieldVisitor = (
        fieldConfig: GraphQLFieldConfig<any, any>,
        fieldName: string,
        typeName: string
      ) => {
        const directives = getDirectives(schema, fieldConfig)

        const directive = directives?.find((d) => d.name === directiveName)

        if (directive) {
          const baseName = getBaseType(fieldConfig.type.toString())
          paginationTypes[baseName] = true
          markedLocations[`${typeName}.${fieldName}`] = baseName + 'Pagination'
        }

        handleCacheControlDirectiveVisitation(
          fieldConfig,
          fieldConfig.type.toString()
        )

        return undefined
      }
      mapSchema(schema, {
        [MapperKind.TYPE]: (type) => {
          handleCacheControlDirectiveVisitation(type, type.name)
          foundTypes[type.name] = type
          return undefined
        },
        [MapperKind.INTERFACE_FIELD]: fieldVisitor,
        [MapperKind.OBJECT_FIELD]: fieldVisitor,
      })

      // Construct new types:

      if (!foundTypes['PageInfo']) {
        newTypeDefs.push(`
          type PaginationInfo {
            hasMore: Boolean!
            hasNew: Int!
            countNew: Int!
            nextOffset: Int!
            nextOffsetRelativeTo: String!
          }
        `)
      }

      if (!foundTypes['PaginationOrdering']) {
        newTypeDefs.push(`
          input PaginationOrdering {
            index: String!,
            direction: String!,
          }
        `)
      }

      for (const name of Object.keys(paginationTypes)) {
        // This applies the cacheControl to Edge type and edges, pageInfo fields
        // The cacheControl is not applied to a Pagination and Node types
        // to comply with GraphQL List cacheControl behavior which has disabled cache by default
        const maxAge = paginationTypeGreatestMaxAge[name]
        const needsCacheControl =
          options?.useCacheControl && typeof maxAge === 'number'
        const cacheControl = needsCacheControl
          ? ` @cacheControl(maxAge: ${maxAge})`
          : ''

        const newPaginationName = `${name}Pagination`
        if (!foundTypes[newPaginationName]) {
          newTypeDefs.push(`
            type ${newPaginationName} {
              nodes: [${name}!]!${cacheControl}
              info: PaginationInfo!${cacheControl}
            }
          `)
        }
      }

      schema = mergeSchemas({
        schemas: [schema],
        typeDefs: newTypeDefs,
      })

      // Rename field types.
      const transformer = (
        typeName: string,
        fieldName: string,
        fieldConfig: GraphQLFieldConfig<any, any>
      ) => {
        const mark = markedLocations[`${typeName}.${fieldName}`]
        if (mark) {
          fieldConfig.type = makePaginationType(fieldConfig.type)
          fieldConfig.args = {
            ...fieldConfig.args,
            ...makePaginationArgs(),
          }
          const remainingDirectives = fieldConfig?.astNode?.directives?.filter(
            (dir) => dir.name.value !== directiveName
          )
          fieldConfig.astNode = {
            ...fieldConfig.astNode,
            directives: remainingDirectives,
          } as any
          return fieldConfig
        } else return undefined
      }
      schema = wrapSchema({
        schema,
        transforms: [
          new TransformInterfaceFields(transformer),
          new TransformObjectFields(transformer),
        ],
      })

      return schema
    },
  }
}

function getBaseType(type?: string): string {
  if (!type) return ''
  if (typeof type !== 'string') return ''
  return type
    .replace(/:/g, '')
    .replace(/\[/g, '')
    .replace(/\]/g, '')
    .replace(/!/g, '')
    .replace(/@/g, '')
    .trim()
}

function makePaginationType(type: GraphQLOutputType): GraphQLOutputType {
  const formattedType = type.toString()
  const baseName = getBaseType(formattedType)
  return new GraphQLNonNull(
    new GraphQLObjectType({
      name: `${baseName}Pagination`,
      fields: {},
    })
  )
}

function makePaginationArgs(): GraphQLFieldConfigArgumentMap {
  return {
    offset: {
      type: new GraphQLNonNull(
        new GraphQLScalarType({
          name: 'Int',
        })
      ),
    },
    limit: {
      type: new GraphQLNonNull(
        new GraphQLScalarType({
          name: 'Int',
        })
      ),
    },
    countNewLimit: {
      type: new GraphQLScalarType({
        name: 'Int',
      }),
    },
    orderings: {
      type: new GraphQLNonNull(
        new GraphQLList(
          new GraphQLNonNull(
            new GraphQLInputObjectType({
              name: 'PaginationOrdering',
              fields: {
                index: { type: new GraphQLScalarType({ name: 'String' }) },
                direction: { type: new GraphQLScalarType({ name: 'String' }) },
              },
            })
          )
        )
      ),
    },
    offsetRelativeTo: {
      type: new GraphQLScalarType({
        name: 'String',
      }),
    },
  }
}
