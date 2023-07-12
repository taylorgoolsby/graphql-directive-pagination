import test from 'boxtape'
import pagination from '../lib/resolver.js'
import sinon from 'sinon'

const posts = []

async function insertPost() {
  const lastPost = posts[posts.length - 1]
  const post = lastPost ? {id: lastPost.id + 1, dateCreated: lastPost.dateCreated + 1} : {id: 0, dateCreated: 0}
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

async function getPosts() {
  return [...posts]
}

const postsResolver = async (user) => {
  return await getPosts()
}

let offset = 0
let limit = 2
let offsetRelativeTo = null
let lastRes = null
let items = []

test('page load request on empty data source', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()
    const rows = await postsResolver(parent)
    return rows
  })

  offset = 0
  offsetRelativeTo = null // null offsetRelativeTo is the definition of a page load request.

  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countLoaded: 0})
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
    const rows = await postsResolver(parent)
    return rows
  })

  offset = -2
  offsetRelativeTo = null // null offsetRelativeTo is the definition of a page load request.

  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countLoaded: 0})
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
    const rows = await postsResolver(parent)
    return rows
  })

  offset = -2
  offsetRelativeTo = null // null offsetRelativeTo is the definition of a page load request.

  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countLoaded: 0})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.moreOffset, 2, 'moreOffset 2')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(9), 'nextOffsetRelativeTo is greatest dateCreated')
  t.equal(callSpy.callCount, 1, 'one call was made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('page load request on non-zero starting page', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()
    const rows = await postsResolver(parent)
    return rows
  })

  offset = limit * 2
  offsetRelativeTo = null // null offsetRelativeTo is the definition of a page load request.

  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countLoaded: 0})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 5, 'row 5 found')
  t.equal(res.nodes[1].id, 4, 'row 4 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.moreOffset, 6, 'moreOffset 6')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(9), 'nextOffsetRelativeTo is greatest dateCreated')
  t.equal(callSpy.callCount, 1, 'one call was made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
})

test('page load request', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()
    const rows = await postsResolver(parent)
    return rows
  })

  offset = 0
  offsetRelativeTo = null

  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countLoaded: 0})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.moreOffset, 2, 'moreOffset 2')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(9), 'nextOffsetRelativeTo is greatest dateCreated')
  t.equal(callSpy.callCount, 1, 'one call was made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  items = res.nodes
})

test('load more request', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()
    const rows = await postsResolver(parent, args)
    return rows
  })


  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countLoaded: items.length})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 7, 'row 7 found')
  t.equal(res.nodes[1].id, 6, 'row 6 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 0')
  t.equal(res.info.moreOffset, 4, 'moreOffset 4')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(9), 'nextOffsetRelativeTo unchanged')
  t.equal(callSpy.callCount, 1, 'one call was made')
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
    const rows = await postsResolver(parent, args)
    return rows
  })

  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countLoaded: items.length})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 5, 'row 5 found')
  t.equal(res.nodes[1].id, 4, 'row 4 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, true, 'hasNew true')
  t.equal(res.info.countNew, 2, 'countNew 2')
  t.equal(res.info.moreOffset, 6, 'moreOffset 6')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(9), 'nextOffsetRelativeTo unchanged')
  t.equal(callSpy.callCount, 1, 'one call was made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  items = [...items, ...res.nodes]
})

test('load new request', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()
    const rows = await postsResolver(parent, args)
    return rows
  })

  const tempOffset = offset
  offset = -lastRes.info.countNew
  limit = lastRes.info.countNew
  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countLoaded: items.length})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 11, 'row 11 found')
  t.equal(res.nodes[1].id, 10, 'row 10 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, false, 'hasNew false')
  t.equal(res.info.countNew, 0, 'countNew 2')
  t.equal(res.info.moreOffset, 8, 'moreOffset 8')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(11), 'nextOffsetRelativeTo reset')
  t.equal(callSpy.callCount, 1, 'one call was made')
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  offset = tempOffset + lastRes.info.countNew
  lastRes = res
  items = [...res.nodes, ...items]
})

test('add two more posts and continue load more where left off', async (t) => {
  const callSpy = sinon.spy()

  await insertPost()
  await insertPost()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()
    const rows = await postsResolver(parent, args)
    return rows
  })


  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countLoaded: items.length})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 3, 'row 3 found')
  t.equal(res.nodes[1].id, 2, 'row 2 found')
  t.equal(res.info.hasMore, true, 'hasMore true')
  t.equal(res.info.hasNew, true, 'hasNew true')
  t.equal(res.info.countNew, 2, 'countNew 2')
  t.equal(res.info.moreOffset, 10, 'moreOffset 10')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(11), 'nextOffsetRelativeTo unchanged')
  t.equal(callSpy.callCount, 1, 'one call was made')
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
    const rows = await postsResolver(parent, args)
    return rows
  })

  const tempOffset = offset
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
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(13), 'nextOffsetRelativeTo reset')
  t.equal(callSpy.callCount, 1, 'one call was made')
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  offset = tempOffset + lastRes.info.countNew
  lastRes = res
  items = [...res.nodes, ...items]
})

test('final load more request (has more false)', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()
    const rows = await postsResolver(parent, args)
    return rows
  })


  const res = await wrappedResolver({}, {offset, limit, orderings, offsetRelativeTo, countLoaded: items.length})
  t.equal(res.nodes.length, 2, 'limit respected')
  t.equal(res.nodes[0].id, 1, 'row 1 found')
  t.equal(res.nodes[1].id, 0, 'row 0 found')
  t.equal(res.info.hasMore, false, 'hasMore true')
  t.equal(res.info.hasNew, true, 'hasNew false')
  t.equal(res.info.countNew, 2, 'countNew 2')
  // Even though hasMore is false, moreOffset is still advanced in case client wants to query after waiting some time.
  t.equal(res.info.moreOffset, 14, 'moreOffset 14')
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(13), 'nextOffsetRelativeTo unchanged')
  t.equal(callSpy.callCount, 1, 'one call was made')
  lastRes = res
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  items = [...items, ...res.nodes]
})

test('final load new request', async (t) => {
  const callSpy = sinon.spy()

  const wrappedResolver = pagination(async (parent, args) => {
    callSpy()
    const rows = await postsResolver(parent, args)
    return rows
  })

  const tempOffset = offset
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
  t.equal(res.info.nextOffsetRelativeTo, JSON.stringify(15), 'nextOffsetRelativeTo reset')
  t.equal(callSpy.callCount, 1, 'calls made')
  offset = res.info.moreOffset
  offsetRelativeTo = res.info.nextOffsetRelativeTo
  offset = tempOffset + lastRes.info.countNew
  lastRes = res
  items = [...res.nodes, ...items]

  // verify items was reconstructed correctly:
  items.reverse()
  for (let i = 0; i < items.length; i++) {
    t.equal(items[i].id, i, 'item order')
  }
})