// import { sqltag } from "sql-template-tag";
// @ts-ignore
import mysql from 'mysql'
import { IFieldResolver } from '@graphql-tools/utils'
import { DateTime } from 'luxon'

export type PaginationResult<T> = {
  nodes: T[]
  info: {
    hasMore: boolean
    hasNew: boolean
    countNew: number
    moreOffset: number
    nextOffsetRelativeTo: string | null
  }
}

type Ordering = {
  index: string
  direction: string
}

type Clauses = {
  mysql: {
    where?: string
    orderBy: string
    limit: string
  }
  postgres: {
    where?: string
    orderBy: string
    offset: string
    limit: string
  }
}

export interface PaginationArgs {
  offset: number
  limit: number
  countNewLimit?: number
  orderings: Ordering[]
  countLoaded: number
  offsetRelativeTo?: string

  clauses: Clauses
}

type WrappedResolver<T> = IFieldResolver<any, any, PaginationArgs, Promise<T[]>>

type OffsetRelativeTo = string | number | Date | null

const usedSymbol = Symbol('used')
const typeSymbol = Symbol('type')

export function configurePagination(timezone: string) {
  function escape(template: string, values: any[]): string {
    // This relies on the fact that for our use cases,
    // the WHERE and ORDER BY clauses are the same in MySQL and PostgresQL.
    return mysql.format(template, values)
  }

  function toJSDate(timestamp: any): Date | null {
    if (timestamp instanceof Date || typeof timestamp === 'string') {
      if (typeof timestamp === 'string' && /^\d+$/.test(timestamp)) {
        // This is a string only containing numbers, so attempt to parse it as a unix millis timestamp.
        timestamp = parseInt(timestamp, 10)
      }
      const a = new Date(timestamp)
      if (isNaN(a.valueOf())) {
        return null
      } else {
        return a
      }
    } else {
      return null
    }
  }

  function toSQLTimestamp(timestamp: any): string | null {
    const a = toJSDate(timestamp)
    if (a) {
      const dt = DateTime.fromJSDate(a, { zone: timezone })
      if (!dt.isValid) {
        return null
      } else {
        return dt.toSQL({ includeOffset: false })
      }
    } else {
      return null
    }
  }

  function getNegativeOffsetArgs(
    args: PaginationArgs,
    offsetRelativeTo: any
  ): PaginationArgs {
    const offsetRelativeToAsTimestamp = toSQLTimestamp(offsetRelativeTo)

    // const limit = args.limit + 1 // limit is padded in order to detect if there is next page.
    // @ts-ignore
    const orderBy = args.orderings.map((o) =>
      escape(`?? ${o.direction.toUpperCase() === 'DESC' ? 'ASC' : 'DESC'}`, [
        o.index,
      ])
    )
    const desc = args.orderings[0].direction.toUpperCase() === 'DESC'

    const offset = 0
    // Only lookahead by this limit to prevent case where there are
    // hundred of new rows and then node server has to process a lot of rows.
    // By default, this limit is determined by args.limit, but the client can override this limit just for this
    // special case using countNewLimit
    const limit = args.countNewLimit || args.limit

    const clauses = {
      mysql: {
        // @ts-ignore
        where: escape(`?? ${desc ? '>' : '<'} ?`, [
          args.orderings[0].index,
          offsetRelativeToAsTimestamp || offsetRelativeTo,
        ]),
        // @ts-ignore
        orderBy: orderBy.join(', '),
        limit: escape('?, ?', [offset, limit]),
      },
      postgres: {
        // @ts-ignore
        where: escape(`?? ${desc ? '>' : '<'} ?`, [
          args.orderings[0].index,
          offsetRelativeToAsTimestamp || offsetRelativeTo,
        ]),
        // @ts-ignore
        orderBy: orderBy.join(', '),
        offset: escape('?', [offset]),
        limit: escape('?', [limit]),
      },
    }

    let used = false
    const proxyClauses = new Proxy(clauses, {
      get: (target, prop) => {
        if (prop === usedSymbol) {
          return used
        }
        if (prop === typeSymbol) {
          return 'negative'
        }
        used = true
        // @ts-ignore
        return target[prop]
      },
    })

    const reverseOrderings = args.orderings.map((ordering) => ({
      index: ordering.index,
      direction: ordering.direction.toUpperCase() === 'DESC' ? 'ASC' : 'DESC',
    }))

    return {
      ...args,
      offset,
      limit,
      orderings: reverseOrderings,
      clauses: proxyClauses,
      offsetRelativeTo: JSON.stringify(offsetRelativeTo),
    }
  }

  function getPositiveOffsetArgs(
    args: PaginationArgs,
    offsetRelativeTo: any
  ): PaginationArgs {
    const offsetRelativeToAsTimestamp = toSQLTimestamp(offsetRelativeTo)

    const offset = args.offset
    const limit = args.limit // limit is padded in order to detect if there is next page.
    // @ts-ignore
    const orderBy = args.orderings.map((o) =>
      escape(`?? ${o.direction.toUpperCase() === 'DESC' ? 'DESC' : 'ASC'}`, [
        o.index,
      ])
    )

    const desc = args.orderings[0].direction.toUpperCase() === 'DESC'

    const clauses = {
      mysql: {
        // @ts-ignore
        where: escape(`?? ${desc ? '<=' : '>='} ?`, [
          args.orderings[0].index,
          offsetRelativeToAsTimestamp || offsetRelativeTo,
        ]),
        // @ts-ignore
        orderBy: orderBy.join(', '),
        limit: escape('?, ?', [offset, limit]),
      },
      postgres: {
        // @ts-ignore
        where: escape(`?? ${desc ? '<=' : '>='} ?`, [
          args.orderings[0].index,
          offsetRelativeToAsTimestamp || offsetRelativeTo,
        ]),
        // @ts-ignore
        orderBy: orderBy.join(', '),
        offset: escape('?', [offset]),
        limit: escape('?', [limit]),
      },
    }

    let used = false
    const proxyClauses = new Proxy(clauses, {
      get: (target, prop) => {
        if (prop === usedSymbol) {
          return used
        }
        if (prop === typeSymbol) {
          return 'positive'
        }
        used = true
        // @ts-ignore
        return target[prop]
      },
    })

    return {
      ...args,
      offset,
      limit,
      clauses: proxyClauses,
      offsetRelativeTo: JSON.stringify(offsetRelativeTo),
    }
  }

  function getRootArgs(args: PaginationArgs): PaginationArgs {
    // @ts-ignore
    const orderBy = args.orderings.map((o) =>
      escape(`?? ${o.direction.toUpperCase() === 'DESC' ? 'DESC' : 'ASC'}`, [
        o.index,
      ])
    )

    const clauses = {
      mysql: {
        // @ts-ignore
        where: '',
        // @ts-ignore
        orderBy: orderBy.join(', '),
        limit: `0, 1`,
      },
      postgres: {
        // @ts-ignore
        where: '',
        // @ts-ignore
        orderBy: orderBy.join(', '),
        offset: `0`,
        limit: `1`,
      },
    }

    let used = false
    const proxyClauses = new Proxy(clauses, {
      get: (target, prop) => {
        if (prop === usedSymbol) {
          return used
        }
        if (prop === typeSymbol) {
          return 'root'
        }
        used = true
        // @ts-ignore
        return target[prop]
      },
    })

    return {
      ...args,
      offset: 0,
      limit: 1,
      clauses: proxyClauses,
      offsetRelativeTo: JSON.stringify(null),
    }
  }

  async function determineOffsetRelativeTo<T>(
    memoR: WrappedResolver<T>,
    parent,
    args,
    ctx,
    info
  ): Promise<OffsetRelativeTo> {
    const rootArgs = getRootArgs(args)
    const nodes = await memoR(parent, rootArgs, ctx, info)

    if (!nodes || !Array.isArray(nodes)) {
      throw new Error('Expected resolver to return an array.')
    }

    if (!nodes.length) {
      return null
    }

    const offsetRelativeTo = nodes?.[0]?.[args.orderings[0].index]
    if (!offsetRelativeTo && typeof offsetRelativeTo !== 'number') {
      throw new Error(
        `Unable to find a value for column "${args.orderings[0].index}".`
      )
    }
    return offsetRelativeTo
  }

  async function determineHasMore<T>(
    memoR: WrappedResolver<T>,
    parent: any,
    args: PaginationArgs,
    ctx: any,
    info: any,
    determinedOffsetRelativeTo: OffsetRelativeTo,
    originalOffsetRelativeTo: OffsetRelativeTo,
    // These are the final nodes which will be included in the response:
    foundNodes: T[]
  ): Promise<{
    hasMore: boolean
    moreOffsetRelativeToOrignal: number
  }> {
    // hasMore controls if a button at the end of the pagination is available
    // to load more rows.

    // If the request did not specify offsetRelativeTo, then countLoaded should have been 0.
    const offsetRelativeToExists =
      !!originalOffsetRelativeTo || typeof originalOffsetRelativeTo === 'number'
    const countLoaded = offsetRelativeToExists ? args.countLoaded : 0

    // These calculated offsets are relative to the request's offsetRelativeTo.

    // The offset of the last row the client has loaded:
    const currentOffsetLast = countLoaded - 1
    // After this response, it will be the offset of the last row the client has loaded:
    const nextOffsetLast = args.offset + foundNodes.length - 1
    // Diff these two offsets:
    const diff = Math.max(nextOffsetLast - currentOffsetLast, 0)
    // After this response, it will be the client's countLoaded relative to the original offsetRelativeTo:
    const nextCountLoaded = countLoaded + diff

    const positiveArgs = getPositiveOffsetArgs(
      {
        ...args,
        offset: nextCountLoaded,
        limit: 1,
      },
      determinedOffsetRelativeTo
    )
    const nodes = await memoR(parent, positiveArgs, ctx, info)
    return {
      hasMore: !!nodes.length,
      moreOffsetRelativeToOrignal: nextCountLoaded,
    }
  }

  const emptyResult: PaginationResult<any> = {
    nodes: [],
    info: {
      // If the number of negative offset rows is equal to or larger than the page size,
      // then the current offsetRoot is the next row after the negative offset rows,
      // and that must occur on the next page, so there must be a next page.
      hasMore: false,
      hasNew: false,
      countNew: 0,
      // The node[0] has this offset value:
      moreOffset: 0,
      nextOffsetRelativeTo: JSON.stringify(null),
    },
  }

  function noClauseSort(nodes: any[], args: PaginationArgs) {
    nodes.sort((a, b) => {
      const aValue =
        toJSDate(a[args.orderings[0].index]) ?? a[args.orderings[0].index]
      const bValue =
        toJSDate(b[args.orderings[0].index]) ?? b[args.orderings[0].index]

      let orderingsIndex = 0
      while (args.orderings[orderingsIndex]) {
        // Keep trying to find a difference between a and b with respect to
        // the orderings.
        if (aValue < bValue) {
          return args.orderings[orderingsIndex].direction.toUpperCase() ===
            'DESC'
            ? 1
            : -1
        }
        if (aValue > bValue) {
          return args.orderings[orderingsIndex].direction.toUpperCase() ===
            'DESC'
            ? -1
            : 1
        }

        orderingsIndex++
      }

      // If we have gone through all orderings without finding a difference, then
      // they are equal.
      return 0
    })
    return nodes
  }

  // Returned arrays from memoR are always sorted, offsetted, and limited,
  // but in the case where clauses are not used, memoization is used so that,
  // only one call to the underlying resolver is ever made.
  function makeMemoizedResolver<T>(
    r: WrappedResolver<T>,
    originalArgs: PaginationArgs
  ): WrappedResolver<T> {
    let memoizedValue
    let useMemo = false
    return async (p, a: PaginationArgs, c, i) => {
      if (useMemo) {
        // memoizedValue = noClauseSort(memoizedValue, a)
        return noClauseOffsetAndLimit(memoizedValue, a)
      }
      const nodes = await r(p, a, c, i)

      // @ts-ignore
      if (!a.clauses[usedSymbol]) {
        useMemo = true
        memoizedValue = noClauseSort(nodes, originalArgs)
        return noClauseOffsetAndLimit(memoizedValue, a)
      }

      return nodes
    }
  }

  function noClauseOffsetAndLimit<T>(
    noClauseNodes: T[],
    args: PaginationArgs
  ): T[] {
    // Pretend to be the database, doing the where, offset and limit on an already sorted array.
    const type = args.clauses[typeSymbol]

    const offsetRelativeTo: OffsetRelativeTo = args.offsetRelativeTo
      ? JSON.parse(args.offsetRelativeTo)
      : null

    // Apply offset and limit
    const offsetRelativeToExists =
      !!offsetRelativeTo || typeof offsetRelativeTo === 'number'
    // @ts-ignore
    let offsetRootIndex: number = offsetRelativeToExists
      ? noClauseNodes.findIndex(
          (node) => node[args.orderings[0].index] === offsetRelativeTo
        )
      : -1
    if (offsetRootIndex === -1) {
      offsetRootIndex = 0
    }
    const offsetIndex = offsetRootIndex + args.offset

    if (type === 'negative') {
      // To be consistent with the algorithm for when clauses are used,
      // this function should return all rows associated with a negative offset
      // according to the args.offset and args.limit.
      const limitedNodes = noClauseNodes.slice(
        Math.max(offsetIndex - args.limit, 0),
        offsetIndex
      )
      return limitedNodes.reverse()
    } else {
      const limitedNodes = noClauseNodes.slice(
        offsetIndex,
        offsetIndex + args.limit
      )
      return limitedNodes
    }
  }

  return function resolver<T>(
    r: WrappedResolver<T>
  ): IFieldResolver<any, any, PaginationArgs, Promise<PaginationResult<T>>> {
    return async (parent, args: PaginationArgs, ctx, info) => {
      if (!args?.orderings?.[0]) {
        // There has to be at least 1 ordering
        // return emptyResult
        throw Error('There must be at least one ordering.')
      }

      const memoR = makeMemoizedResolver(r, args)

      let offsetRelativeTo: OffsetRelativeTo = args.offsetRelativeTo
        ? JSON.parse(args.offsetRelativeTo)
        : null
      const originalOffsetRelativeTo = offsetRelativeTo

      if (!offsetRelativeTo && args.offset < 0) {
        // Since getting the offsetRelativeTo on page load means getting the first row in the DB,
        // this should "redirect" to the case of:
        // args.offset: 0
        // offsetRelativeTo: null
        args.offset = 0
      }

      if (!offsetRelativeTo) {
        offsetRelativeTo = await determineOffsetRelativeTo(
          memoR,
          parent,
          args,
          ctx,
          info
        )
        if (!offsetRelativeTo) {
          return emptyResult
        }
      }

      if (args.offset < 0) {
        const negativeArgs = getNegativeOffsetArgs(
          {
            ...args,
            countNewLimit: args.limit,
          },
          offsetRelativeTo
        )
        const nodes = await memoR(parent, negativeArgs, ctx, info)

        // The nodes returned from negativeOffsetClauses is reversed.
        const startIndex = Math.max(nodes.length + args.offset, 0)
        const slicedNodes = nodes
          .reverse()
          .slice(startIndex, startIndex + args.limit)

        const nextOffsetRelativeTo =
          slicedNodes[0]?.[args.orderings[0].index] ?? offsetRelativeTo

        // Get the count of rows associated with negative offset as countNew:
        const newRowArgs = getNegativeOffsetArgs(args, nextOffsetRelativeTo)
        const negativeNodes = await memoR(parent, newRowArgs, ctx, info)

        const { hasMore, moreOffsetRelativeToOrignal } = await determineHasMore(
          memoR,
          parent,
          args,
          ctx,
          info,
          offsetRelativeTo,
          originalOffsetRelativeTo,
          slicedNodes
        )

        const result: PaginationResult<T> = {
          nodes: slicedNodes,
          info: {
            // If the number of negative offset rows is equal to or larger than the page size,
            // then the current offsetRoot is the next row after the negative offset rows,
            // and that must occur on the next page, so there must be a next page.
            hasMore,
            // hasNew: startIndex > 0,
            // countNew: Math.max(startIndex, 0),
            hasNew: !!negativeNodes.length,
            countNew: negativeNodes.length,
            // If there is hasMore, it is found at moreOffset relative to nextOffsetRelativeTo.
            // This calculation relies on slicedNodes not containing any non-negative offset rows
            // relative to the originalOffsetRelativeTo.
            // These slicedNodes are being added before the originalOffsetRelativeTo,
            // so the difference in places between the nextOffsetRelativeTo and the original is added.
            moreOffset: moreOffsetRelativeToOrignal + slicedNodes.length,
            nextOffsetRelativeTo: JSON.stringify(nextOffsetRelativeTo),
          },
        }

        return result
      } else {
        // args.offset >= 0
        const positiveArgs = getPositiveOffsetArgs(args, offsetRelativeTo)
        const nodes = await memoR(parent, positiveArgs, ctx, info)

        // Get the count of rows associated with negative offset as countNew:
        const negativeArgs = getNegativeOffsetArgs(args, offsetRelativeTo)
        const negativeNodes = await memoR(parent, negativeArgs, ctx, info)

        const { hasMore, moreOffsetRelativeToOrignal } = await determineHasMore(
          memoR,
          parent,
          args,
          ctx,
          info,
          offsetRelativeTo,
          originalOffsetRelativeTo,
          nodes
        )

        const result: PaginationResult<T> = {
          nodes,
          info: {
            hasMore,
            hasNew: !!negativeNodes.length,
            countNew: negativeNodes.length,
            // nextOffsetRelativeTo is equal to the original.
            moreOffset: moreOffsetRelativeToOrignal,
            nextOffsetRelativeTo: JSON.stringify(offsetRelativeTo),
          },
        }

        return result
      }
    }
  }
}
