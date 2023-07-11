// import { sqltag } from "sql-template-tag";
// @ts-ignore
import mysql from 'mysql'
import { IFieldResolver } from '@graphql-tools/utils'

export type PaginationResult<T> = {
  nodes: T[]
  info: {
    hasMore: boolean
    hasNew: boolean
    countNew: number
    nextOffset: number
    nextOffsetRelativeTo: string | null
  }
}

type Ordering = {
  index: string
  direction: string
}

type Clauses = {
  mysql: {
    where: string
    orderBy: string
    limit: string
  }
  postgres: {
    where: string
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
  offsetRelativeTo?: string

  clauses: Clauses
}

type WrappedResolver<T> = IFieldResolver<any, any, PaginationArgs, Promise<T[]>>

type OffsetRelativeTo = string | number | Date | null

function escape(template: string, values: any[]): string {
  // This relies on the fact that for our use cases,
  // the WHERE and ORDER BY clauses are the same in MySQL and PostgresQL.
  // console.log('template.sql', template.sql)
  // console.log('template.values', template.values)
  // console.log('mysql.format(template.sql, template.values)', mysql.format(template.sql, template.values))
  return mysql.format(template, values)
}

function getNegativeOffsetClauses(
  args: PaginationArgs,
  offsetRelativeTo: any
): Clauses {
  // const limit = args.limit + 1 // limit is padded in order to detect if there is next page.
  // @ts-ignore
  const orderBy = args.orderings.map((o) =>
    escape(`?? ${o.direction.toUpperCase() === 'DESC' ? 'ASC' : 'DESC'}`, [
      o.index,
    ])
  )
  const desc = args.orderings[0].direction.toUpperCase() === 'DESC'

  // Only lookahead by this limit to prevent case where there are
  // hundred of new rows and then node server has to process a lot of rows.
  // By default, this limit is determined by args.limit, but the client can override this limit just for this
  // special case using countNewLimit
  const limit = args.countNewLimit || args.limit

  const result = {
    mysql: {
      // @ts-ignore
      where: escape(`?? ${desc ? '>' : '<'} ?`, [
        args.orderings[0].index,
        offsetRelativeTo,
      ]),
      // @ts-ignore
      orderBy: orderBy.join(', '),
      limit: escape('0, ?', [limit]),
    },
    postgres: {
      // @ts-ignore
      where: escape(`?? ${desc ? '>' : '<'} ?`, [
        args.orderings[0].index,
        offsetRelativeTo,
      ]),
      // @ts-ignore
      orderBy: orderBy.join(', '),
      offset: '0',
      limit: escape('?', [limit]),
    },
  }

  let used = false
  return new Proxy(result, {
    get: (target, prop) => {
      if (prop === '__used') {
        return used
      }
      used = true
      // @ts-ignore
      return target[prop]
    },
  })
}

function getClauses(args: PaginationArgs, offsetRelativeTo: any): Clauses {
  const offset = args.offset
  const limit = args.limit + 1 // limit is padded in order to detect if there is next page.
  // @ts-ignore
  const orderBy = args.orderings.map((o) =>
    escape(`?? ${o.direction.toUpperCase() === 'DESC' ? 'DESC' : 'ASC'}`, [
      o.index,
    ])
  )

  const desc = args.orderings[0].direction.toUpperCase() === 'DESC'

  const result = {
    mysql: {
      // @ts-ignore
      where: escape(`?? ${desc ? '<=' : '>='} ?`, [
        args.orderings[0].index,
        offsetRelativeTo,
      ]),
      // @ts-ignore
      orderBy: orderBy.join(', '),
      limit: escape('?, ?', [offset, limit]),
    },
    postgres: {
      // @ts-ignore
      where: escape(`?? ${desc ? '<=' : '>='} ?`, [
        args.orderings[0].index,
        offsetRelativeTo,
      ]),
      // @ts-ignore
      orderBy: orderBy.join(', '),
      offset: escape('?', [offset]),
      limit: escape('?', [limit]),
    },
  }

  let used = false
  return new Proxy(result, {
    get: (target, prop) => {
      if (prop === '__used') {
        return used
      }
      used = true
      // @ts-ignore
      return target[prop]
    },
  })
}

function getRootClauses(args: PaginationArgs): Clauses {
  // @ts-ignore
  const orderBy = args.orderings.map((o) =>
    escape(`?? ${o.direction.toUpperCase() === 'DESC' ? 'DESC' : 'ASC'}`, [
      o.index,
    ])
  )

  const result = {
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
  return new Proxy(result, {
    get: (target, prop) => {
      if (prop === '__used') {
        return used
      }
      used = true
      // @ts-ignore
      return target[prop]
    },
  })
}

async function determineOffsetRelativeToOrNodes<T>(
  parent,
  args,
  ctx,
  info,
  wrappedResolver: WrappedResolver<T>,
  offsetRelativeTo: OffsetRelativeTo
): Promise<{
  offsetRelativeTo: OffsetRelativeTo | null
  nodes: T[] | null
  clausesUsed: boolean
}> {
  // If clauses are being used, then an offsetRelativeTo will be determined.
  // Otherwise, if clauses are not being used, we assume the wrapped resolver returns all rows from the DB source,
  // where sorting, offsetting, and limiting has not been applied.

  let nodes: T[] | null = null
  let clausesUsed = false
  if (!offsetRelativeTo) {
    const rootClauses = getRootClauses(args)
    const tempNodes = await wrappedResolver(
      parent,
      {
        ...args,
        clauses: rootClauses,
      },
      ctx,
      info
    )

    // @ts-ignore
    clausesUsed = rootClauses.__used
    if (clausesUsed) {
      // If clauses were used, then it is expected that tempNodes.length === 1,
      // and that will now be designated as the offsetRelativeTo.
      if (!tempNodes || !tempNodes[0]) {
        // The caller should `return emptyResult` if it finds this "empty" return value:
        return {
          offsetRelativeTo: null,
          nodes: null,
          clausesUsed,
        }
      }

      offsetRelativeTo = tempNodes[0][args.orderings[0].index]
      if (!offsetRelativeTo && typeof offsetRelativeTo !== 'number') {
        throw new Error(
          `Unable to find a value for column "${args.orderings[0].index}".`
        )
      }
    } else {
      // If clauses were not used, then getRootClauses was ignored, meaning tempNodes should be the
      // full table of data.
      nodes = tempNodes ?? []
      // In this case, when clauses are not used, offsetRelativeTo is determined later, after we sort the nodes.
    }
  }

  return {
    offsetRelativeTo: offsetRelativeTo ?? null,
    nodes,
    clausesUsed,
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
    nextOffset: 0,
    nextOffsetRelativeTo: JSON.stringify(null),
  },
}

export default function resolver<T>(
  r: WrappedResolver<T>
): IFieldResolver<any, any, PaginationArgs, Promise<PaginationResult<T>>> {
  return async (parent, args: PaginationArgs, ctx, info) => {
    if (!args.orderings[0]) {
      // There has to be at least 1 ordering
      // return emptyResult
      throw Error('There must be at least one ordering.')
    }

    let offsetRelativeTo: OffsetRelativeTo = args.offsetRelativeTo
      ? JSON.parse(args.offsetRelativeTo)
      : null

    if (!offsetRelativeTo && args.offset < 0) {
      // This should "redirect" to the case of:
      // args.offset: 0
      // offsetRelativeTo: null
      args.offset = 0
    }

    if (args.offset < 0) {
      // Negative offset is used to get new rows,
      // rows that have been added to the DB source after the client has established offsetRelativeTo,
      // AKA rows associated with negative offset.

      // If offset is negative and the request does not specify an offsetRelativeTo,
      // then this server would try to establish an offsetRelativeTo, but this would be the 0th index row,
      // which means there are no rows with negative offset, so we return empty, but with a nextOffsetRelativeTo.
      // if (!offsetRelativeTo) {
      //   const initializationRes = await determineOffsetRelativeToOrNodes(parent, args, ctx, info, r, offsetRelativeTo)
      //   // If an offsetRelativeTo or nodes were found, then there are more.
      //   const hasMore = (initializationRes.clausesUsed && initializationRes.offsetRelativeTo !== null) || (!initializationRes.clausesUsed && !!initializationRes.nodes?.length)
      //   return {
      //     ...emptyResult,
      //     info: {
      //       ...emptyResult.info,
      //       hasMore,
      //       nextOffsetRelativeTo: JSON.stringify(initializationRes.offsetRelativeTo ?? null)
      //     }
      //   }
      // }

      // This newRowsClause will preform a query for all rows prior to offsetRootValue.
      const clauses = getNegativeOffsetClauses(args, offsetRelativeTo)
      const nodes = await r(
        parent,
        {
          ...args,
          clauses,
        },
        ctx,
        info
      )

      // @ts-ignore
      const clausesUsed = clauses.__used

      if (clausesUsed) {
        // If clauses are used for negative offset,
        // then it is expected that the sort, limit and offset are already applied.

        // nodes contains all new rows, which is equivalent to doing a query of offset: -countNew.
        // However, it is possible to set offset to a negative value in the range [-countNew, 0).
        const startIndex = nodes.length + args.offset
        const slicedNodes = nodes
          .reverse()
          .slice(startIndex, startIndex + args.limit)

        // Since the offset is negative, there must be more.
        const hasMore = true

        const result: PaginationResult<any> = {
          nodes: slicedNodes,
          info: {
            // If the number of negative offset rows is equal to or larger than the page size,
            // then the current offsetRoot is the next row after the negative offset rows,
            // and that must occur on the next page, so there must be a next page.
            hasMore,
            hasNew: startIndex !== 0,
            countNew: startIndex,
            // The node[0] has this offset value:
            nextOffset: 0,
            nextOffsetRelativeTo: JSON.stringify(
              slicedNodes[0]?.[args.orderings[0].index] ?? null
            ),
          },
        }

        return result
      } else {
        // It is expected that the sort, limit and offset have not been applied.

        if (!nodes || !Array.isArray(nodes)) {
          throw new Error('The pagination resolver should return an array.')
        }

        // Apply sort:
        nodes.sort((a, b) => {
          let orderingsIndex = 0

          while (args.orderings[orderingsIndex]) {
            // Keep trying to find a difference between a and b with respect to
            // the orderings.

            if (a[args.orderings[0].index] < b[args.orderings[0].index]) {
              return args.orderings[0].direction.toUpperCase() === 'DESC'
                ? 1
                : -1
            }
            if (a[args.orderings[0].index] > b[args.orderings[0].index]) {
              return args.orderings[0].direction.toUpperCase() === 'DESC'
                ? -1
                : 1
            }
            orderingsIndex++
          }

          // If we have gone through all orderings without finding a difference, then
          // they are equal.
          return 0
        })

        // Apply offset and limit
        const offsetRelativeToGiven =
          !!offsetRelativeTo || typeof offsetRelativeTo === 'number'
        // @ts-ignore
        let offsetRootIndex: number = offsetRelativeToGiven
          ? nodes.findIndex(
              (node) => node[args.orderings[0].index] === offsetRelativeTo
            )
          : -1
        if (offsetRootIndex === -1) {
          offsetRootIndex = 0
        }

        const offsetIndex = offsetRootIndex + args.offset
        // To be consistent with the algorithm for negative offsets with clauses,
        // the limitedNodes does not include rows which are equal to or come after the offsetRootIndex.
        const limit = Math.min(args.limit, Math.abs(args.offset))
        const limitedNodes = nodes.slice(offsetIndex, offsetIndex + limit)

        const result: PaginationResult<any> = {
          nodes: limitedNodes,
          info: {
            // If the number of negative offset rows is equal to or larger than the page size,
            // then the current offsetRoot is the next row after the negative offset rows,
            // and that must occur on the next page, so there must be a next page.
            hasMore: true,
            hasNew: offsetIndex > 0,
            countNew: offsetIndex,

            // The node[0] has this offset value:
            nextOffset: 0,
            nextOffsetRelativeTo: JSON.stringify(
              limitedNodes[0]?.[args.orderings[0].index] ?? null
            ),
          },
        }

        return result
      }
    } else {
      // args.offset is 0 or positive

      let nodes: T[] | null
      if (!offsetRelativeTo) {
        const initializationRes = await determineOffsetRelativeToOrNodes(
          parent,
          args,
          ctx,
          info,
          r,
          offsetRelativeTo
        )
        if (
          (initializationRes.clausesUsed &&
            initializationRes.offsetRelativeTo === null) ||
          (!initializationRes.clausesUsed && !initializationRes.nodes?.length)
        ) {
          // In both cases, whether clauses were used or not,
          // when the DB source is empty, we will not be able to determine an offsetRelativeTo or nodes.
          // Therefore, return empty.
          return emptyResult
        }
        offsetRelativeTo = initializationRes.offsetRelativeTo
        nodes = initializationRes.nodes
      }

      let clausesUsed = false
      // @ts-ignore
      if (!nodes) {
        // nodes could have been determined by now if the request did not specify a
        // offsetRelativeTo and clauses aren't being used
        const clauses = getClauses(args, offsetRelativeTo)
        // @ts-ignore
        nodes = await r(
          parent,
          {
            ...args,
            // @ts-ignore
            offsetRelativeTo,
            clauses,
          },
          ctx,
          info
        )
        // @ts-ignore
        clausesUsed = clauses.__used

        if (!nodes || !Array.isArray(nodes)) {
          throw new Error('The pagination resolver should return an array.')
        }
      }

      if (clausesUsed) {
        // If clauses are used, then nodes is already sorted, offsetted, and limited.

        // Get the count of rows associated with negative offset as countNew:
        // @ts-ignore
        const negativeOffsetRows = await r(
          parent,
          {
            ...args,
            // @ts-ignore
            offsetRelativeTo,
            clauses: getNegativeOffsetClauses(args, offsetRelativeTo),
          },
          ctx,
          info
        )

        let hasMore = false
        if (nodes.length === args.limit + 1) {
          hasMore = true
          nodes.pop()
        }

        const result: PaginationResult<any> = {
          nodes,
          info: {
            hasMore,
            hasNew: !!negativeOffsetRows.length,
            countNew: negativeOffsetRows.length,
            nextOffset: args.offset,
            nextOffsetRelativeTo: JSON.stringify(offsetRelativeTo),
          },
        }

        return result
      } else {
        // If clauses are not used, then it is expected that the resolver
        // returns all rows.
        // IOW, sort, offset, and limit have not been applied yet.

        // Apply sort:
        nodes.sort((a, b) => {
          let orderingsIndex = 0

          while (args.orderings[orderingsIndex]) {
            // Keep trying to find a difference between a and b with respect to
            // the orderings.

            if (a[args.orderings[0].index] < b[args.orderings[0].index]) {
              return args.orderings[0].direction.toUpperCase() === 'DESC'
                ? 1
                : -1
            }
            if (a[args.orderings[0].index] > b[args.orderings[0].index]) {
              return args.orderings[0].direction.toUpperCase() === 'DESC'
                ? -1
                : 1
            }
            orderingsIndex++
          }

          // If we have gone through all orderings without finding a difference, then
          // they are equal.
          return 0
        })

        // Apply offset and limit
        const offsetRelativeToGiven =
          !!offsetRelativeTo || typeof offsetRelativeTo === 'number'
        // @ts-ignore
        let offsetRootIndex = offsetRelativeToGiven
          ? nodes.findIndex(
              (node) => node[args.orderings[0].index] === offsetRelativeTo
            )
          : -1
        if (offsetRootIndex === -1) {
          offsetRootIndex = 0
        }
        const nextOffsetRelativeTo =
          nodes[offsetRootIndex]?.[args.orderings[0].index] ?? null

        const offsetIndex = offsetRootIndex + args.offset
        const limitedNodes = nodes.slice(offsetIndex, offsetIndex + args.limit)

        /*
         nodes:        -------
         limitedNodes:    ^--
        * */

        const result: PaginationResult<any> = {
          nodes: limitedNodes,
          info: {
            hasMore: nodes.length - offsetIndex > limitedNodes.length,
            hasNew: offsetRootIndex > 0,
            countNew: offsetRootIndex,

            // When the client receives these values, it should accept them.
            // offset = nextOffset
            nextOffset: args.offset,
            // offsetRelativeTo = nextOffsetRelativeTo
            nextOffsetRelativeTo: JSON.stringify(nextOffsetRelativeTo),
          },
        }

        return result
      }
    }
  }
}
