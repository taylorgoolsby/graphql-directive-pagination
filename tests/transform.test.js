import test from 'boxtape'
import {makeExecutableSchema} from '@graphql-tools/schema'
import { printSchemaWithDirectives } from '@graphql-tools/utils'
import paginationDirective from '../lib/index.js'

const {
  paginationDirectiveTypeDefs,
  paginationDirectiveTransform,
} = paginationDirective('pagination')

test('transform', (t) => {
  const typeDefs = `
    directive @sql on FIELD_DEFINITION

    type User {
      userId: Int
      posts: [Post!]! @sql @pagination
    }

    type Post {
      postId: Int
    }

    type Query {
      user: User
    }
  `
  const expected = `schema {
  query: Query
}

directive @pagination on FIELD_DEFINITION

directive @sql on FIELD_DEFINITION

type Query {
  user: User
}

type User {
  userId: Int
  posts(offset: Int!, limit: Int!, countNewLimit: Int, orderings: [PaginationOrdering!]!, countLoaded: Int!, offsetRelativeTo: String): PostPagination! @sql
}

type Post {
  postId: Int
}

type PaginationInfo {
  hasMore: Boolean!
  hasNew: Boolean!
  countNew: Int!
  moreOffset: Int!
  nextOffsetRelativeTo: String!
}

input PaginationOrdering {
  index: String!
  direction: String!
}

type PostPagination {
  nodes: [Post!]!
  info: PaginationInfo!
}`
  runTest(t, typeDefs, expected)
})

function runTest(t, typeDefs, expected) {
  const resolvers = {}
  let schema = makeExecutableSchema({
    typeDefs: [paginationDirectiveTypeDefs, typeDefs],
    resolvers
  })
  schema = paginationDirectiveTransform(schema, resolvers)
  const answer = printSchemaWithDirectives(schema)

  if (answer !== expected) {
    console.log('answer', answer)
  }

  t.equal(answer, expected)
}
