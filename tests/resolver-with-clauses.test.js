import test from 'boxtape'
import pagination from '../lib/resolver.js'
import sinon from 'sinon'
import sqlite3 from 'sqlite3'
import {open} from 'sqlite'

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

const orderings = [{index: 'dateCreated', direction: 'desc'}, {index: 'id', direction: 'desc'}]

const typeDefs = `
  type User {
    id: Int 
    test: [Something!]! @pagination
    posts: [Post!]!
  }
  
  type Something {
    id: Int
  }

  type Post {
    id: Int
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
  const rows = await db.all(query)
  return rows
}

const postsResolver = async (user, args) => {
  return await getPosts(args.clauses.mysql)
}

let offset = 0
let limit = 2
let offsetRelativeTo = null
let lastRes = null

test('page load request on empty data source', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(args.offset, 0, 'offset arg')
    t.equal(args.limit, 2, 'limit arg')
    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getOffsetRelativeTo call')
      t.equal(args.clauses.mysql.limit, '0, 1', 'mysql limit arg, getOffsetRelativeTo call')

      t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getOffsetRelativeTo call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getOffsetRelativeTo call')
      t.equal(args.clauses.postgres.limit, '1', 'postgres limit arg, getOffsetRelativeTo call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getOffsetRelativeTo finds 0 rows')
      return rows
    }
  })

  offset = 0
  offsetRelativeTo = null // null offsetRelativeTo is the definition of a page load request.

  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo})
  t.equal(res.nodes.length, 0, 'limit respected')
  t.equal(res.info.hasMore, false, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.nextOffset, 0, 'nextOffset 0')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(null), 'nextOffsetRelativeTo is null')
  t.equal(callSpy.callCount, 1, 'one call was made')
  lastRes = res
  offset = res.info.nextOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('page load request on empty data source with negative offset', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(args.offset, 0, 'offset arg')
    t.equal(args.limit, 2, 'limit arg')
    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getOffsetRelativeTo call')
      t.equal(args.clauses.mysql.limit, '0, 1', 'mysql limit arg, getOffsetRelativeTo call')

      t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getOffsetRelativeTo call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getOffsetRelativeTo call')
      t.equal(args.clauses.postgres.limit, '1', 'postgres limit arg, getOffsetRelativeTo call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getOffsetRelativeTo finds 0 rows')
      return rows
    }
  })

  offset = -2
  offsetRelativeTo = null // null offsetRelativeTo is the definition of a page load request.

  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo})
  t.equal(res.nodes.length, 0, 'limit respected')
  t.equal(res.info.hasMore, false, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.nextOffset, 0, 'nextOffset 0')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(null), 'nextOffsetRelativeTo is null')
  t.equal(callSpy.callCount, 1, 'one call was made')
  lastRes = res
  offset = res.info.nextOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('page load request with negative offset', async (t) => {
  // The response of this case should be equivalent to a
  // page load with offset: 0.
  const callSpy = sinon.spy()

  for (let i = 0; i < 10; i++) {
    await insertPost()
  }

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(args.offset, 0, 'offset arg')
    t.equal(args.limit, 2, 'limit arg')
    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getOffsetRelativeTo call')
      t.equal(args.clauses.mysql.limit, '0, 1', 'mysql limit arg, getOffsetRelativeTo call')

      t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getOffsetRelativeTo call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getOffsetRelativeTo call')
      t.equal(args.clauses.postgres.limit, '1', 'postgres limit arg, getOffsetRelativeTo call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'getOffsetRelativeTo finds 1 row')
      t.equal(rows[0].id, 9, 'getOffsetRelativeTo finds row id: 9')
      return rows
    } else if (callSpy.callCount === 2) {
      t.equal(args.clauses.mysql.where, '`dateCreated` <= 9', 'mysql where arg, getPositiveRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getPositiveRows call')
      t.equal(args.clauses.mysql.limit, '0, 3', 'mysql limit arg, getPositiveRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` <= 9', 'postgres where arg, getPositiveRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getPositiveRows call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getPositiveRows call')
      t.equal(args.clauses.postgres.limit, '3', 'postgres limit arg, getPositiveRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 3, 'getPositiveRows finds 3 rows (1 extra row)')
      t.equal(rows[0].id, 9, 'getPositiveRows finds row 9')
      t.equal(rows[1].id, 8, 'getPositiveRows finds row 8')
      t.equal(rows[2].id, 7, 'getPositiveRows finds row 7')
      return rows
    } else {
      t.equal(args.clauses.mysql.where, '`dateCreated` > 9', 'mysql where arg, getNegativeRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` ASC, `id` ASC', 'mysql orderBy arg, getNegativeRows call')
      t.equal(args.clauses.mysql.limit, '0, 2', 'mysql limit arg, getNegativeRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` > 9', 'postgres where arg, getNegativeRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` ASC, `id` ASC', 'postgres orderBy arg, getNegativeRows call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getNegativeRows call')
      t.equal(args.clauses.postgres.limit, '2', 'postgres limit arg, getNegativeRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getNegativeRows finds no rows')
      return rows
    }
  })

  offset = -2
  offsetRelativeTo = null // null offsetRelativeTo is the definition of a page load request.

  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.nextOffset, 0, 'nextOffset 0')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(9), 'nextOffsetRelativeTo is greatest dateCreated')
  t.equal(callSpy.callCount, 3, 'three calls were made')
  lastRes = res
  offset = res.info.nextOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('page load request on non-zero starting page', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(args.offset, 4, 'offset arg')
    t.equal(args.limit, 2, 'limit arg')
    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getOffsetRelativeTo call')
      t.equal(args.clauses.mysql.limit, '0, 1', 'mysql limit arg, getOffsetRelativeTo call')

      t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getOffsetRelativeTo call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getOffsetRelativeTo call')
      t.equal(args.clauses.postgres.limit, '1', 'postgres limit arg, getOffsetRelativeTo call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'getOffsetRelativeTo finds 1 row')
      t.equal(rows[0].id, 9, 'getOffsetRelativeTo finds row id: 9')
      return rows
    } else if (callSpy.callCount === 2) {
      t.equal(args.clauses.mysql.where, '`dateCreated` <= 9', 'mysql where arg, getPositiveRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getPositiveRows call')
      t.equal(args.clauses.mysql.limit, '4, 3', 'mysql limit arg, getPositiveRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` <= 9', 'postgres where arg, getPositiveRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getPositiveRows call')
      t.equal(args.clauses.postgres.offset, '4', 'postgres offset arg, getPositiveRows call')
      t.equal(args.clauses.postgres.limit, '3', 'postgres limit arg, getPositiveRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 3, 'getPositiveRows finds 3 rows (1 extra row)')
      t.equal(rows[0].id, 5, 'getPositiveRows finds row 5')
      t.equal(rows[1].id, 4, 'getPositiveRows finds row 4')
      t.equal(rows[2].id, 3, 'getPositiveRows finds row 3')
      return rows
    } else {
      t.equal(args.clauses.mysql.where, '`dateCreated` > 9', 'mysql where arg, getNegativeRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` ASC, `id` ASC', 'mysql orderBy arg, getNegativeRows call')
      t.equal(args.clauses.mysql.limit, '0, 2', 'mysql limit arg, getNegativeRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` > 9', 'postgres where arg, getNegativeRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` ASC, `id` ASC', 'postgres orderBy arg, getNegativeRows call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getNegativeRows call')
      t.equal(args.clauses.postgres.limit, '2', 'postgres limit arg, getNegativeRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getNegativeRows finds no rows')
      return rows
    }
  })


  offset = limit * 2
  offsetRelativeTo = null // null offsetRelativeTo is the definition of a page load request.

  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 5, 'row 5 found')
  t.equal(res.nodes[1].id, 4, 'row 4 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.nextOffset, 4, 'nextOffset 4')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(9), 'nextOffsetRelativeTo is greatest dateCreated')
  t.equal(callSpy.callCount, 3, 'three calls were made')
  lastRes = res
  offset = res.info.nextOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('page load request', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(args.offset, 0, 'offset arg')
    t.equal(args.limit, 2, 'limit arg')
    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getOffsetRelativeTo call')
      t.equal(args.clauses.mysql.limit, '0, 1', 'mysql limit arg, getOffsetRelativeTo call')

      t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getOffsetRelativeTo call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getOffsetRelativeTo call')
      t.equal(args.clauses.postgres.limit, '1', 'postgres limit arg, getOffsetRelativeTo call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'getOffsetRelativeTo finds 1 row')
      t.equal(rows[0].id, 9, 'getOffsetRelativeTo finds row id: 9')
      return rows
    } else if (callSpy.callCount === 2) {
      t.equal(args.clauses.mysql.where, '`dateCreated` <= 9', 'mysql where arg, getPositiveRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getPositiveRows call')
      t.equal(args.clauses.mysql.limit, '0, 3', 'mysql limit arg, getPositiveRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` <= 9', 'postgres where arg, getPositiveRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getPositiveRows call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getPositiveRows call')
      t.equal(args.clauses.postgres.limit, '3', 'postgres limit arg, getPositiveRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 3, 'getPositiveRows finds 3 rows (1 extra row)')
      t.equal(rows[0].id, 9, 'getPositiveRows finds row 9')
      t.equal(rows[1].id, 8, 'getPositiveRows finds row 8')
      t.equal(rows[2].id, 7, 'getPositiveRows finds row 7')
      return rows
    } else {
      t.equal(args.clauses.mysql.where, '`dateCreated` > 9', 'mysql where arg, getNegativeRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` ASC, `id` ASC', 'mysql orderBy arg, getNegativeRows call')
      t.equal(args.clauses.mysql.limit, '0, 2', 'mysql limit arg, getNegativeRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` > 9', 'postgres where arg, getNegativeRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` ASC, `id` ASC', 'postgres orderBy arg, getNegativeRows call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getNegativeRows call')
      t.equal(args.clauses.postgres.limit, '2', 'postgres limit arg, getNegativeRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getNegativeRows finds no rows')
      return rows
    }
  })

  offset = 0
  offsetRelativeTo = null

  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.nextOffset, 0, 'nextOffset 0')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(9), 'nextOffsetRelativeTo is greatest dateCreated')
  t.equal(callSpy.callCount, 3, 'three calls were made')
  lastRes = res
  offset = res.info.nextOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('load more request', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(args.offset, 2, 'offset arg')
    t.equal(args.limit, 2, 'limit arg')
    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      t.equal(args.clauses.mysql.where, '`dateCreated` <= 9', 'mysql where arg, getPositiveRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getPositiveRows call')
      t.equal(args.clauses.mysql.limit, '2, 3', 'mysql limit arg, getPositiveRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` <= 9', 'postgres where arg, getPositiveRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getPositiveRows call')
      t.equal(args.clauses.postgres.offset, '2', 'postgres offset arg, getPositiveRows call')
      t.equal(args.clauses.postgres.limit, '3', 'postgres limit arg, getPositiveRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 3, 'getPositiveRows finds 3 rows (1 extra row)')
      t.equal(rows[0].id, 7, 'getPositiveRows finds row 7')
      t.equal(rows[1].id, 6, 'getPositiveRows finds row 6')
      t.equal(rows[2].id, 5, 'getPositiveRows finds row 5')
      return rows
    } else if (callSpy.callCount === 2) {
      t.equal(args.clauses.mysql.where, '`dateCreated` > 9', 'mysql where arg, getNegativeRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` ASC, `id` ASC', 'mysql orderBy arg, getNegativeRows call')
      t.equal(args.clauses.mysql.limit, '0, 2', 'mysql limit arg, getNegativeRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` > 9', 'postgres where arg, getNegativeRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` ASC, `id` ASC', 'postgres orderBy arg, getNegativeRows call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getNegativeRows call')
      t.equal(args.clauses.postgres.limit, '2', 'postgres limit arg, getNegativeRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getNegativeRows finds no rows')
      return rows
    }
  })

  offset += limit
  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 7, 'row 7 found')
  t.equal(res.nodes[1].id, 6, 'row 6 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.nextOffset, 2, 'nextOffset 2')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(9), 'nextOffsetRelativeTo unchanged')
  t.equal(callSpy.callCount, 2, 'two calls were made')
  lastRes = res
  offset = res.info.nextOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('load more request after new rows have been added', async (t) => {
  const callSpy = sinon.spy()

  await insertPost()
  await insertPost()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(args.offset, 4, 'offset arg')
    t.equal(args.limit, 2, 'limit arg')
    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      t.equal(args.clauses.mysql.where, '`dateCreated` <= 9', 'mysql where arg, getPositiveRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getPositiveRows call')
      t.equal(args.clauses.mysql.limit, '4, 3', 'mysql limit arg, getPositiveRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` <= 9', 'postgres where arg, getPositiveRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getPositiveRows call')
      t.equal(args.clauses.postgres.offset, '4', 'postgres offset arg, getPositiveRows call')
      t.equal(args.clauses.postgres.limit, '3', 'postgres limit arg, getPositiveRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 3, 'getPositiveRows finds 3 rows (1 extra row)')
      t.equal(rows[0].id, 5, 'getPositiveRows finds row 5')
      t.equal(rows[1].id, 4, 'getPositiveRows finds row 4')
      t.equal(rows[2].id, 3, 'getPositiveRows finds row 3')
      return rows
    } else if (callSpy.callCount === 2) {
      t.equal(args.clauses.mysql.where, '`dateCreated` > 9', 'mysql where arg, getNegativeRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` ASC, `id` ASC', 'mysql orderBy arg, getNegativeRows call')
      t.equal(args.clauses.mysql.limit, '0, 2', 'mysql limit arg, getNegativeRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` > 9', 'postgres where arg, getNegativeRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` ASC, `id` ASC', 'postgres orderBy arg, getNegativeRows call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getNegativeRows call')
      t.equal(args.clauses.postgres.limit, '2', 'postgres limit arg, getNegativeRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getNegativeRows finds rows')
      t.equal(rows[0].id, 10, 'row 10 is new')
      t.equal(rows[1].id, 11, 'row 11 is new')
      return rows
    }
  })

  offset += limit
  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 5, 'row 5 found')
  t.equal(res.nodes[1].id, 4, 'row 4 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, true, 'hasNew true')
  t.equal(res.info.countNew, 2, 'countNew 2')
  t.equal(res.info.nextOffset, 4, 'nextOffset 4')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(9), 'nextOffsetRelativeTo unchanged')
  t.equal(callSpy.callCount, 2, 'two calls were made')
  lastRes = res
  offset = res.info.nextOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('load new request', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(args.offset, -2, 'offset arg')
    t.equal(args.limit, 2, 'limit arg')
    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      t.equal(args.clauses.mysql.where, '`dateCreated` > 9', 'mysql where arg, getNegativeRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` ASC, `id` ASC', 'mysql orderBy arg, getNegativeRows call')
      t.equal(args.clauses.mysql.limit, '0, 2', 'mysql limit arg, getNegativeRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` > 9', 'postgres where arg, getNegativeRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` ASC, `id` ASC', 'postgres orderBy arg, getNegativeRows call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getNegativeRows call')
      t.equal(args.clauses.postgres.limit, '2', 'postgres limit arg, getNegativeRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getNegativeRows finds rows')
      t.equal(rows[0].id, 10, 'row 10 is new')
      t.equal(rows[1].id, 11, 'row 11 is new')
      return rows
    }
  })

  const tempOffset = offset
  offset = -lastRes.info.countNew
  limit = lastRes.info.countNew
  const res = await wrappedResolver({}, {offset, limit: 2, orderings, offsetRelativeTo})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 11, 'row 11 found')
  t.equal(res.nodes[1].id, 10, 'row 10 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 2')
  t.equal(res.info.nextOffset, 0, 'nextOffset 2')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(11), 'nextOffsetRelativeTo reset')
  t.equal(callSpy.callCount, 1, 'one call was made')
  offset = res.info.nextOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  offset = tempOffset + lastRes.info.countNew
  lastRes = res
})

test('add two more posts and continue load more where left off', async (t) => {
  const callSpy = sinon.spy()

  await insertPost()
  await insertPost()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(args.offset, 8, 'offset arg')
    t.equal(args.limit, 2, 'limit arg')
    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      t.equal(args.clauses.mysql.where, '`dateCreated` <= 11', 'mysql where arg, getPositiveRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getPositiveRows call')
      t.equal(args.clauses.mysql.limit, '8, 3', 'mysql limit arg, getPositiveRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` <= 11', 'postgres where arg, getPositiveRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getPositiveRows call')
      t.equal(args.clauses.postgres.offset, '8', 'postgres offset arg, getPositiveRows call')
      t.equal(args.clauses.postgres.limit, '3', 'postgres limit arg, getPositiveRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 3, 'getPositiveRows finds 3 rows (1 extra row)')
      t.equal(rows[0].id, 3, 'getPositiveRows finds row 3')
      t.equal(rows[1].id, 2, 'getPositiveRows finds row 2')
      t.equal(rows[2].id, 1, 'getPositiveRows finds row 1')
      return rows
    } else if (callSpy.callCount === 2) {
      t.equal(args.clauses.mysql.where, '`dateCreated` > 11', 'mysql where arg, getNegativeRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` ASC, `id` ASC', 'mysql orderBy arg, getNegativeRows call')
      t.equal(args.clauses.mysql.limit, '0, 2', 'mysql limit arg, getNegativeRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` > 11', 'postgres where arg, getNegativeRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` ASC, `id` ASC', 'postgres orderBy arg, getNegativeRows call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getNegativeRows call')
      t.equal(args.clauses.postgres.limit, '2', 'postgres limit arg, getNegativeRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getNegativeRows finds rows')
      t.equal(rows[0].id, 12, 'row 12 is new')
      t.equal(rows[1].id, 13, 'row 13 is new')
      return rows
    }
  })

  offset += limit
  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 3, 'row 3 found')
  t.equal(res.nodes[1].id, 2, 'row 2 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, true, 'hasNew true')
  t.equal(res.info.countNew, 2, 'countNew 2')
  t.equal(res.info.nextOffset, 8, 'nextOffset 2')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(11), 'nextOffsetRelativeTo unchanged')
  t.equal(callSpy.callCount, 2, 'two calls were made')
  lastRes = res
  offset = res.info.nextOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('load new request while db added new rows', async (t) => {
  const callSpy = sinon.spy()

  await insertPost()
  await insertPost()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(args.offset, -2, 'offset arg')
    t.equal(args.limit, 2, 'limit arg')
    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      t.equal(args.clauses.mysql.where, '`dateCreated` > 11', 'mysql where arg, getNegativeRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` ASC, `id` ASC', 'mysql orderBy arg, getNegativeRows call')
      t.equal(args.clauses.mysql.limit, '0, 4', 'mysql limit arg, getNegativeRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` > 11', 'postgres where arg, getNegativeRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` ASC, `id` ASC', 'postgres orderBy arg, getNegativeRows call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getNegativeRows call')
      t.equal(args.clauses.postgres.limit, '4', 'postgres limit arg, getNegativeRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 4, 'getNegativeRows finds rows')
      t.equal(rows[0].id, 12, 'row 12 is new')
      t.equal(rows[1].id, 13, 'row 13 is new')
      return rows
    }
  })

  const tempOffset = offset
  offset = -lastRes.info.countNew
  limit = lastRes.info.countNew
  const res = await wrappedResolver({}, {offset, limit: 2, orderings, offsetRelativeTo, countNewLimit: 4})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 13, 'row 13 found')
  t.equal(res.nodes[1].id, 12, 'row 12 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, true, 'hasNew true')
  t.equal(res.info.countNew, 2, 'countNew 2')
  t.equal(res.info.nextOffset, 0, 'nextOffset 2')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(13), 'nextOffsetRelativeTo reset')
  t.equal(callSpy.callCount, 1, 'one call was made')
  offset = res.info.nextOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  offset = tempOffset + lastRes.info.countNew
  lastRes = res
})

test('final load more request (has more false)', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(args.offset, 12, 'offset arg')
    t.equal(args.limit, 2, 'limit arg')
    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      t.equal(args.clauses.mysql.where, '`dateCreated` <= 13', 'mysql where arg, getPositiveRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getPositiveRows call')
      t.equal(args.clauses.mysql.limit, '12, 3', 'mysql limit arg, getPositiveRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` <= 13', 'postgres where arg, getPositiveRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getPositiveRows call')
      t.equal(args.clauses.postgres.offset, '12', 'postgres offset arg, getPositiveRows call')
      t.equal(args.clauses.postgres.limit, '3', 'postgres limit arg, getPositiveRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getPositiveRows finds 2 rows (no extra row)')
      t.equal(rows[0].id, 1, 'getPositiveRows finds row 1')
      t.equal(rows[1].id, 0, 'getPositiveRows finds row 0')
      return rows
    } else if (callSpy.callCount === 2) {
      t.equal(args.clauses.mysql.where, '`dateCreated` > 13', 'mysql where arg, getNegativeRows call')
      t.equal(args.clauses.mysql.orderBy, '`dateCreated` ASC, `id` ASC', 'mysql orderBy arg, getNegativeRows call')
      t.equal(args.clauses.mysql.limit, '0, 2', 'mysql limit arg, getNegativeRows call')

      t.equal(args.clauses.mysql.where, '`dateCreated` > 13', 'postgres where arg, getNegativeRows call')
      t.equal(args.clauses.postgres.orderBy, '`dateCreated` ASC, `id` ASC', 'postgres orderBy arg, getNegativeRows call')
      t.equal(args.clauses.postgres.offset, '0', 'postgres offset arg, getNegativeRows call')
      t.equal(args.clauses.postgres.limit, '2', 'postgres limit arg, getNegativeRows call')

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getNegativeRows finds rows')
      t.equal(rows[0].id, 14, 'row 14 is new')
      t.equal(rows[1].id, 15, 'row 15 is new')
      return rows
    }
  })

  offset += limit
  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 1, 'row 1 found')
  t.equal(res.nodes[1].id, 0, 'row 0 found')
  t.equal(res.info.hasMore, false, 'hasMore true')
  t.equal(res.info.hasNew, true, 'hasNew false')
  t.equal(res.info.countNew, 2, 'countNew 2')
  t.equal(res.info.nextOffset, 12, 'nextOffset 12')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(13), 'nextOffsetRelativeTo unchanged')
  t.equal(callSpy.callCount, 2, 'two calls were made')
  lastRes = res
  offset = res.info.nextOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})
