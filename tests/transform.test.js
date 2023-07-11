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

type User {
  userId: Int
  posts(offset: Int!, limit: Int!, countNewLimit: Int, orderings: [PaginationOrdering!]!, offsetRelativeTo: String): PostPagination! @sql
}

type Post {
  postId: Int
}

type Query {
  user: User
}

type PaginationInfo {
  hasMore: Boolean!
  hasNew: Int!
  countNew: Int!
  nextOffset: Int!
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
  let schema = makeExecutableSchema({
    typeDefs: [paginationDirectiveTypeDefs, typeDefs],
  })
  schema = paginationDirectiveTransform(schema)
  const answer = printSchemaWithDirectives(schema)

  if (answer !== expected) {
    console.log('answer', answer)
  }

  t.equal(answer, expected)
}
