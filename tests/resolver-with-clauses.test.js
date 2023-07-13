import test from 'boxtape'
import paginationDirective from '../lib/index.js'
import sinon from 'sinon'
import sqlite3 from 'sqlite3'
import {open} from 'sqlite'

const {paginationResolver: pagination} = paginationDirective('pagination', {
  timezone: 'utc'
})

console.log('pagination', pagination)

const db = await open({
  filename: ':memory:',
  driver: sqlite3.Database
})

const posts = []

await db.exec(`
  CREATE TABLE Posts (
    id INT NOT NULL,
    userId INT NOT NULL,
    dateCreated TIMESTAMP NOT NULL
  );
`)

function getTimestamp(id) {
  return `2023-07-13 00:00:${id.toString().padStart(2, '0')}.000`
}

function toISO(timestamp) {
  timestamp = timestamp.replace(' ', 'T')
  timestamp = timestamp + 'Z'
  return timestamp
}

async function insertPost() {
  const lastPost = posts[posts.length - 1]
  const post = lastPost ? {id: lastPost.id + 1, dateCreated: getTimestamp(lastPost.id + 1)} : {id: 0, dateCreated: getTimestamp(0)}
  const query = `
    INSERT INTO Posts (
      id,
      userId,
      dateCreated
    ) VALUES (
      ${post.id},
      0,
      '${post.dateCreated}'
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
  for (const item of rows) {
    item.dateCreated = toISO(item.dateCreated)
  }
  return rows
}

const postsResolver = async (user, args) => {
  return await getPosts(args.clauses.mysql)
}

let offset = 0
let limit = 2
let offsetRelativeTo = null
let lastRes = null
let items = []

function testGetOffsetRelativeToClauses(t, args) {
  t.equal(args.offset, 0, 'offset arg')
  t.equal(args.limit, 1, 'limit arg')

  t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getOffsetRelativeTo call')
  t.equal(args.clauses.mysql.limit, `0, 1`, 'mysql limit arg, getOffsetRelativeTo call')

  t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getOffsetRelativeTo call')
  t.equal(args.clauses.postgres.offset, `0`, 'postgres offset arg, getOffsetRelativeTo call')
  t.equal(args.clauses.postgres.limit, `1`, 'postgres limit arg, getOffsetRelativeTo call')
}

function testPositiveClauses(t, args, offsetRelativeTo, expectedOffset, expectedLimit) {
  offsetRelativeTo = getTimestamp(offsetRelativeTo)

  t.equal(args.offset, expectedOffset, 'offset arg')
  t.equal(args.limit, expectedLimit, 'limit arg')

  t.equal(args.clauses.mysql.where, `\`dateCreated\` <= '${offsetRelativeTo}'`, 'mysql where arg, getPositiveRows call')
  t.equal(args.clauses.mysql.orderBy, '`dateCreated` DESC, `id` DESC', 'mysql orderBy arg, getPositiveRows call')
  t.equal(args.clauses.mysql.limit, `${expectedOffset}, ${expectedLimit}`, 'mysql limit arg, getPositiveRows call')

  t.equal(args.clauses.postgres.where, `\`dateCreated\` <= '${offsetRelativeTo}'`, 'postgres where arg, getPositiveRows call')
  t.equal(args.clauses.postgres.orderBy, '`dateCreated` DESC, `id` DESC', 'postgres orderBy arg, getPositiveRows call')
  t.equal(args.clauses.postgres.offset, `${expectedOffset}`, 'postgres offset arg, getPositiveRows call')
  t.equal(args.clauses.postgres.limit, `${expectedLimit}`, 'postgres limit arg, getPositiveRows call')
}

function testNegativeClauses(t, args, offsetRelativeTo, expectedLimit) {
  offsetRelativeTo = getTimestamp(offsetRelativeTo)

  t.equal(args.offset, 0, 'offset arg')
  t.equal(args.limit, expectedLimit, 'limit arg')

  t.equal(args.clauses.mysql.where, `\`dateCreated\` > '${offsetRelativeTo}'`, 'mysql where arg, getNegativeRows call')
  t.equal(args.clauses.mysql.orderBy, '`dateCreated` ASC, `id` ASC', 'mysql orderBy arg, getNegativeRows call')
  t.equal(args.clauses.mysql.limit, `0, ${expectedLimit}`, 'mysql limit arg, getNegativeRows call')

  t.equal(args.clauses.postgres.where, `\`dateCreated\` > '${offsetRelativeTo}'`, 'postgres where arg, getNegativeRows call')
  t.equal(args.clauses.postgres.orderBy, '`dateCreated` ASC, `id` ASC', 'postgres orderBy arg, getNegativeRows call')
  t.equal(args.clauses.postgres.offset, `0`, 'postgres offset arg, getNegativeRows call')
  t.equal(args.clauses.postgres.limit, `${expectedLimit}`, 'postgres limit arg, getNegativeRows call')
}

test('page load request on empty data source', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testGetOffsetRelativeToClauses(t, args)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getOffsetRelativeTo finds 0 rows')
      return rows
    }
  })

  offset = 0
  offsetRelativeTo = null // null offsetRelativeTo is the definition of a page load request.
  limit = 2
  const res = await wrappedResolver({}, {offset, limit, countNewLimit: 4, orderings, offsetRelativeTo, countLoaded: 0})
  t.equal(res.nodes.length, 0, 'limit respected')
  t.equal(res.info.hasMore, false, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.moreOffset, 0, 'moreOffset 0')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(null), 'nextOffsetRelativeTo is null')
  t.equal(callSpy.callCount, 1, 'one call was made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('page load request on empty data source with negative offset', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testGetOffsetRelativeToClauses(t, args)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getOffsetRelativeTo finds 0 rows')
      return rows
    }
  })

  offset = -2
  offsetRelativeTo = null // null offsetRelativeTo is the definition of a page load request.
  limit = 2
  const res = await wrappedResolver({}, {offset, limit, countNewLimit: 4, orderings, offsetRelativeTo, countLoaded: 0})
  t.equal(res.nodes.length, 0, 'limit respected')
  t.equal(res.info.hasMore, false, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.moreOffset, 0, 'moreOffset 0')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(null), 'nextOffsetRelativeTo is null')
  t.equal(callSpy.callCount, 1, 'one call was made')
  lastRes = res
  offset = res.info.moreOffset
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

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testGetOffsetRelativeToClauses(t, args)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'getOffsetRelativeTo finds 1 row')
      t.equal(rows[0].id, 9, 'getOffsetRelativeTo finds row id: 9')
      return rows
    } else if (callSpy.callCount === 2) {
      testPositiveClauses(t, args, 9, 0, 2)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getPositiveRows finds 3 rows (1 extra row)')
      t.equal(rows[0].id, 9, 'getPositiveRows finds row 9')
      t.equal(rows[1].id, 8, 'getPositiveRows finds row 8')
      return rows
    } else if (callSpy.callCount === 3) {
      testNegativeClauses(t, args, 9, 4)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getNegativeRows finds no rows')
      return rows
    } else {
      testPositiveClauses(t, args, 9, 2, 1)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'hasMore finds 1 row')
      t.equal(rows[0].id, 7, 'hasMore finds row 7')
      return rows
    }
  })

  offset = -2
  offsetRelativeTo = null // null offsetRelativeTo is the definition of a page load request.
  limit = 2
  const res = await wrappedResolver({}, {offset, limit, countNewLimit: 4, orderings, offsetRelativeTo, countLoaded: 0})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.moreOffset, 2, 'moreOffset 2')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(toISO(getTimestamp(9))), 'nextOffsetRelativeTo is greatest dateCreated')
  t.equal(callSpy.callCount, 4, 'calls were made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('page load request on non-zero starting page', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testGetOffsetRelativeToClauses(t, args)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'getOffsetRelativeTo finds 1 row')
      t.equal(rows[0].id, 9, 'getOffsetRelativeTo finds row id: 9')
      return rows
    } else if (callSpy.callCount === 2) {
      testPositiveClauses(t, args, 9, 4, 2)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getPositiveRows finds 3 rows (1 extra row)')
      t.equal(rows[0].id, 5, 'getPositiveRows finds row 5')
      t.equal(rows[1].id, 4, 'getPositiveRows finds row 4')
      return rows
    } else if (callSpy.callCount === 3) {
      testNegativeClauses(t, args, 9, 4)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getNegativeRows finds no rows')
      return rows
    } else {
      testPositiveClauses(t, args, 9, 6, 1)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'hasMore finds 1 row')
      t.equal(rows[0].id, 3, 'hasMore finds row 3')
      return rows
    }
  })


  offset = limit * 2
  offsetRelativeTo = null // null offsetRelativeTo is the definition of a page load request.
  limit = 2
  const res = await wrappedResolver({}, {offset, limit, countNewLimit: 4, orderings, offsetRelativeTo, countLoaded: 0})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 5, 'row 5 found')
  t.equal(res.nodes[1].id, 4, 'row 4 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.moreOffset, 6, 'moreOffset 6')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(toISO(getTimestamp(9))), 'nextOffsetRelativeTo is greatest dateCreated')
  t.equal(callSpy.callCount, 4, 'calls were made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('page load request', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testGetOffsetRelativeToClauses(t, args)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'getOffsetRelativeTo finds 1 row')
      t.equal(rows[0].id, 9, 'getOffsetRelativeTo finds row id: 9')
      return rows
    } else if (callSpy.callCount === 2) {
      testPositiveClauses(t, args, 9, 0, 2)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getPositiveRows finds 3 rows (1 extra row)')
      t.equal(rows[0].id, 9, 'getPositiveRows finds row 9')
      t.equal(rows[1].id, 8, 'getPositiveRows finds row 8')
      return rows
    } else if (callSpy.callCount === 3) {
      testNegativeClauses(t, args, 9, 4)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getNegativeRows finds no rows')
      return rows
    } else {
      testPositiveClauses(t, args, 9, 2, 1)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'hasMore finds 1 row')
      t.equal(rows[0].id, 7, 'hasMore finds row 7')
      return rows
    }
  })

  offset = 0
  offsetRelativeTo = null
  limit = 2
  const res = await wrappedResolver({}, {offset, limit, countNewLimit: 4, orderings, offsetRelativeTo, countLoaded: 0})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.moreOffset, 2, 'moreOffset 2')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(toISO(getTimestamp(9))), 'nextOffsetRelativeTo is greatest dateCreated')
  t.equal(callSpy.callCount, 4, 'calls were made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  items = res.nodes
})

test('load more request', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testPositiveClauses(t, args, 9, 2, 2)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getPositiveRows finds 3 rows (1 extra row)')
      t.equal(rows[0].id, 7, 'getPositiveRows finds row 7')
      t.equal(rows[1].id, 6, 'getPositiveRows finds row 6')
      return rows
    } else if (callSpy.callCount === 2) {
      testNegativeClauses(t, args, 9, 4)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getNegativeRows finds no rows')
      return rows
    } else {
      testPositiveClauses(t, args, 9, 4, 1)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'hasMore finds 1 row')
      t.equal(rows[0].id, 5, 'hasMore finds row 5')
      return rows
    }
  })

  limit = 2
  const res = await wrappedResolver({}, {offset, limit, countNewLimit: 4, orderings, offsetRelativeTo, countLoaded: items.length})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 7, 'row 7 found')
  t.equal(res.nodes[1].id, 6, 'row 6 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.moreOffset, 4, 'moreOffset 4')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(toISO(getTimestamp(9))), 'nextOffsetRelativeTo unchanged')
  t.equal(callSpy.callCount, 3, 'calls were made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  items = [...items, ...res.nodes]
})

test('load more request after new rows have been added', async (t) => {
  const callSpy = sinon.spy()

  await insertPost()
  await insertPost()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testPositiveClauses(t, args, 9, 4, 2)

      console.log('args', args)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getPositiveRows finds 3 rows (1 extra row)')
      t.equal(rows[0].id, 5, 'getPositiveRows finds row 5')
      t.equal(rows[1].id, 4, 'getPositiveRows finds row 4')
      return rows
    } else if (callSpy.callCount === 2) {
      testNegativeClauses(t, args, 9, 4)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getNegativeRows finds rows')
      t.equal(rows[0].id, 10, 'row 10 is new')
      t.equal(rows[1].id, 11, 'row 11 is new')
      return rows
    } else {
      testPositiveClauses(t, args, 9, 6, 1)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'hasMore finds 1 row')
      t.equal(rows[0].id, 3, 'hasMore finds row 3')
      return rows
    }
  })

  limit = 2
  const res = await wrappedResolver({}, {offset, limit, countNewLimit: 4, orderings, offsetRelativeTo, countLoaded: items.length})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 5, 'row 5 found')
  t.equal(res.nodes[1].id, 4, 'row 4 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, true, 'hasNew true')
  t.equal(res.info.countNew, 2, 'countNew 2')
  t.equal(res.info.moreOffset, 6, 'moreOffset 6')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(toISO(getTimestamp(9))), 'nextOffsetRelativeTo unchanged')
  t.equal(callSpy.callCount, 3, 'calls were made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  items = [...items, ...res.nodes]
})

test('load new request', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testNegativeClauses(t, args, 9, 4)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getNegativeRows finds rows')
      t.equal(rows[0].id, 10, 'row 10 is new')
      t.equal(rows[1].id, 11, 'row 11 is new')
      return rows
    } else {
      testPositiveClauses(t, args, 9, 6, 1)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'hasMore finds 1 row')
      t.equal(rows[0].id, 3, 'hasMore finds row 3')
      return rows
    }
  })

  offset = -lastRes.info.countNew
  limit = lastRes.info.countNew
  const res = await wrappedResolver({}, {offset, limit, countNewLimit: 4, orderings, offsetRelativeTo, countLoaded: items.length})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 11, 'row 11 found')
  t.equal(res.nodes[1].id, 10, 'row 10 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 2')
  t.equal(res.info.moreOffset, 8, 'moreOffset 8')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(toISO(getTimestamp(11))), 'nextOffsetRelativeTo reset')
  t.equal(callSpy.callCount, 2, 'call was made')
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  lastRes = res
  items = [...res.nodes, ...items]
})

test('add two more posts and continue load more where left off', async (t) => {
  const callSpy = sinon.spy()

  await insertPost()
  await insertPost()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testPositiveClauses(t, args, 11, 8, 2)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getPositiveRows finds 3 rows (1 extra row)')
      t.equal(rows[0].id, 3, 'getPositiveRows finds row 3')
      t.equal(rows[1].id, 2, 'getPositiveRows finds row 2')
      return rows
    } else if (callSpy.callCount === 2) {
      testNegativeClauses(t, args, 11, 4)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getNegativeRows finds rows')
      t.equal(rows[0].id, 12, 'row 12 is new')
      t.equal(rows[1].id, 13, 'row 13 is new')
      return rows
    } else {
      testPositiveClauses(t, args, 11, 10, 1)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'hasMore finds 1 row')
      t.equal(rows[0].id, 1, 'hasMore finds row 1')
      return rows
    }
  })

  limit = 2
  const res = await wrappedResolver({}, {offset, limit, countNewLimit: 4, orderings, offsetRelativeTo, countLoaded: items.length})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 3, 'row 3 found')
  t.equal(res.nodes[1].id, 2, 'row 2 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, true, 'hasNew true')
  t.equal(res.info.countNew, 2, 'countNew 2')
  t.equal(res.info.moreOffset, 10, 'moreOffset 10')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(toISO(getTimestamp(11))), 'nextOffsetRelativeTo unchanged')
  t.equal(callSpy.callCount, 3, 'calls were made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  items = [...items, ...res.nodes]
})

test('load new request while db added new rows', async (t) => {
  const callSpy = sinon.spy()

  await insertPost()
  await insertPost()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testNegativeClauses(t, args, 11, 4)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 4, 'getNegativeRows finds rows')
      t.equal(rows[0].id, 12, 'row 12 is new')
      t.equal(rows[1].id, 13, 'row 13 is new')
      return rows
    } else {
      testPositiveClauses(t, args, 11, 10, 1)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 1, 'hasMore find 1 row')
      t.equal(rows[0].id, 1, 'hasMore finds row 1')
      return rows
    }
  })

  offset = -lastRes.info.countNew
  limit = lastRes.info.countNew
  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countNewLimit: 4, countLoaded: items.length})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 13, 'row 13 found')
  t.equal(res.nodes[1].id, 12, 'row 12 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, true, 'hasNew true')
  t.equal(res.info.countNew, 2, 'countNew 2')
  t.equal(res.info.moreOffset, 12, 'moreOffset 12')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(toISO(getTimestamp(13))), 'nextOffsetRelativeTo reset')
  t.equal(callSpy.callCount, 2, 'calls made')
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  lastRes = res
  items = [...res.nodes, ...items]
})

test('final load more request (has more false)', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testPositiveClauses(t, args, 13, 12, 2)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getPositiveRows finds 2 rows')
      t.equal(rows[0].id, 1, 'getPositiveRows finds row 1')
      t.equal(rows[1].id, 0, 'getPositiveRows finds row 0')
      return rows
    } else if (callSpy.callCount === 2) {
      testNegativeClauses(t, args, 13, 4)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getNegativeRows finds rows')
      t.equal(rows[0].id, 14, 'row 14 is new')
      t.equal(rows[1].id, 15, 'row 15 is new')
      return rows
    } else {
      testPositiveClauses(t, args, 13, 14, 1)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'hasMore find no rows')
      return rows
    }
  })

  limit = 2
  const res = await wrappedResolver({}, {offset, limit, countNewLimit: 4, orderings, offsetRelativeTo, countLoaded: items.length})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 1, 'row 1 found')
  t.equal(res.nodes[1].id, 0, 'row 0 found')
  t.equal(res.info.hasMore, false, 'hasMore true')
  t.equal(res.info.hasNew, true, 'hasNew false')
  t.equal(res.info.countNew, 2, 'countNew 2')
  // Even though hasMore is false, moreOffset is still advanced in case client wants to query after waiting some time.
  t.equal(res.info.moreOffset, 14, 'moreOffset 14')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(toISO(getTimestamp(13))), 'nextOffsetRelativeTo unchanged')
  t.equal(callSpy.callCount, 3, 'calls were made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  items = [...items, ...res.nodes]
})

test('final load new request', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testNegativeClauses(t, args, 13, 4)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 2, 'getNegativeRows finds rows')
      t.equal(rows[0].id, 14, 'row 14 is new')
      t.equal(rows[1].id, 15, 'row 15 is new')
      return rows
    } else {
      testPositiveClauses(t, args, 13, 14, 1)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'hasMore find no rows')
      return rows
    }
  })

  offset = -lastRes.info.countNew
  limit = lastRes.info.countNew
  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countNewLimit: 4, countLoaded: items.length})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 15, 'row 15 found')
  t.equal(res.nodes[1].id, 14, 'row 14 found')
  t.equal(res.info.hasMore, false, 'hasMore false')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 2')
  t.equal(res.info.moreOffset, 16, 'moreOffset 16')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(toISO(getTimestamp(15))), 'nextOffsetRelativeTo reset')
  t.equal(callSpy.callCount, 2, 'calls made')
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  lastRes = res
  items = [...res.nodes, ...items]

  // verify items was reconstructed correctly:
  items.reverse()
  for (let i = 0; i < items.length; i++) {
    t.equal(items[i].id, i, 'item order')
  }
})

test('load more when there are none', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testPositiveClauses(t, args, 15, 16, 2)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getPositiveRows finds no rows')
      return rows
    } else if (callSpy.callCount === 2) {
      testNegativeClauses(t, args, 15, 4)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getNegativeRows finds no rows')
      return rows
    } else {
      testPositiveClauses(t, args, 15, 16, 1)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'hasMore find no rows')
      return rows
    }
  })

  t.equal(offset, items.length, 'offset is on the imaginary more row')
  t.equal(offsetRelativeTo, JSON.stringify(toISO(getTimestamp(items.length - 1))), 'offsetRelativeTo is at top of list.')
  limit = 2
  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countNewLimit: 4, countLoaded: items.length})
  t.equal(res.nodes.length, 0, 'limit respected')
  t.equal(res.info.hasMore, false, 'hasMore false')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.moreOffset, offset, 'moreOffset does not change')
  t.equal(res.info.nextOffsetRelativeTo, offsetRelativeTo, 'nextOffsetRelativeTo does not change')
  t.equal(callSpy.callCount, 3, 'calls made')
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  lastRes = res
  items = [...items, ...res.nodes]
})

test('load new when there are none', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()

    t.equal(JSON.stringify(args.orderings), JSON.stringify(orderings), 'orderings arg')
    if (callSpy.callCount === 1) {
      testNegativeClauses(t, args, 15, 4)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'getNegativeRows finds no rows')
      return rows
    } else {
      testPositiveClauses(t, args, 15, 16, 1)

      const rows = await postsResolver(parent, args)
      t.equal(rows.length, 0, 'hasMore find no rows')
      return rows
    }
  })

  const originalOffset = offset
  t.equal(offsetRelativeTo, JSON.stringify(toISO(getTimestamp(items.length - 1))), 'offsetRelativeTo is at top of list.')
  limit = 2
  offset = -limit
  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countNewLimit: 4, countLoaded: items.length})
  t.equal(res.nodes.length, 0, 'limit respected')
  t.equal(res.info.hasMore, false, 'hasMore false')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.moreOffset, originalOffset, 'moreOffset does not change')
  t.equal(res.info.nextOffsetRelativeTo, offsetRelativeTo, 'nextOffsetRelativeTo does not change')
  t.equal(callSpy.callCount, 2, 'calls made')
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  lastRes = res
  items = [...res.nodes, ...items]
})