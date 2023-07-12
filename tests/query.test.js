import test from 'boxtape'
import {makeExecutableSchema} from '@graphql-tools/schema'
import paginationDirective from '../lib/index.js'
import {print} from 'graphql'
import gql from 'graphql-tag'
import pagination from '../lib/resolver.js'
import sqlite3 from 'sqlite3'
import {open} from 'sqlite'
import express from 'express'
import { createHandler } from 'graphql-http/lib/use/express';
import fetch from 'cross-fetch'
import bodyParser from "body-parser";

const {
  paginationDirectiveTypeDefs,
  paginationDirectiveTransform,
} = paginationDirective('pagination')

const db = await open({
  filename: ':memory:',
  driver: sqlite3.Database
})

const posts = []

await db.exec(`
  CREATE TABLE Posts (
    id INT NOT NULL,
    userId INT NOT NULL,
    dateCreated INT NOT NULL
  );
`)

async function insertPost() {
  const lastPost = posts[posts.length - 1]
  const post = lastPost ? {id: lastPost.id + 1, dateCreated: lastPost.dateCreated + 1} : {id: 0, dateCreated: 0}
  const query = `
    INSERT INTO Posts (
      id,
      userId,
      dateCreated
    ) VALUES (
      ${post.id},
      0,
      ${post.dateCreated}
    );
  `
  await db.exec(query)
  posts.push(post)
}

for (let i = 0; i < 10; i++) {
  await insertPost()
}

const typeDefs = `
  type User {
    id: Int
    posts: [Post!]! @pagination
  }

  type Something {
    id: Int
    dateCreated: Int
  }

  type Post {
    id: Int
    dateCreated: Int
  }

  type Query {
    user: User
  }
`

async function getPosts(clauses) {
  const query = `
    SELECT * FROM Posts
    WHERE userId = 0
    ${clauses.where ? `AND ${clauses.where}` : ''} 
    ORDER BY ${clauses.orderBy}
    LIMIT ${clauses.limit};
  `
  // console.log('query', query)
  const rows = await db.all(query)
  return rows
}

const postsResolver = async (user, args) => {
  if (args.clauses) {
    return await getPosts(args.clauses.mysql)
  } else {
    return posts
  }
}

const resolvers = {
  User: {
    posts: pagination(postsResolver)
  },
  Query: {
    user: async () => {
      return {
        id: 0
      }
    }
  }
}

let schema = makeExecutableSchema({
  typeDefs: [paginationDirectiveTypeDefs, typeDefs],
  resolvers
})

schema = paginationDirectiveTransform(schema, resolvers)

const query = gql`
  query {
    user {
      id
      posts(offset: 0, limit: 2, orderings: [{index: "dateCreated", direction: "desc"}, {index: "id", direction: "desc"}], countLoaded: 0) {
        nodes {
          id
          dateCreated
        }
        info {
          hasMore
          hasNew
          countNew
          nextOffset
          nextOffsetRelativeTo
        }
      }
    }
  }
`

const app = express()
app.use(bodyParser.json())
app.post('/graphql', createHandler({schema}))
const server = app.listen({port: 55474})

const res = await fetch('http://localhost:55474/graphql', {
  method: 'POST',
  headers: {
    'content-type': 'application/json'
  },
  body: JSON.stringify({
    query: print(query)
  })
})
const data = await res.json()

test('end-to-end query', (t) => {
  t.equals(JSON.stringify(data), `{"data":{"user":{"id":0,"posts":{"nodes":[{"id":9,"dateCreated":9},{"id":8,"dateCreated":8}],"info":{"hasMore":true,"hasNew":false,"countNew":0,"nextOffset":0,"nextOffsetRelativeTo":"9"}}}}}`, 'query output')
})

server.close()
