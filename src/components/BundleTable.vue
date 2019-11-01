<template>
  <table class="bundle-table">
    <thead>
    <tr>
      <th class="bundle-table__timestamp">Timestamp</th>
      <th class="bundle-table__node">Node / ID</th>
      <th class="bundle-table__data">Data</th>
    </tr>
    </thead>
    <transition-group name="list" tag="tbody">
      <tr v-for="m in bundle" :key="m.message_id" class="bundle-table__line">
        <td>{{ m.timestamp }}</td>
        <td>
          {{ m.node_id }} / {{ m.message_id }}
        </td>
        <td>
          <table class="bundle-table__data-values">
            <tbody>
            <tr>
              <th v-for="key in Object.keys(m.data)">{{ key }}</th>
            </tr>
            <tr>
              <td v-for="value in Object.values(m.data)">{{ value }}</td>
            </tr>
            </tbody>
          </table>
        </td>
      </tr>
    </transition-group>
  </table>
</template>

<script>

import BUNDLE_QUERY from '../graphql/bundle.graphql';
import BUNDLE_SUBSCRIPTION from '../graphql/subscription/bundle.graphql';

export default {
  data() {
    return {
      limit: 10,
      bundle: [],
    };
  },

  apollo: {
    bundle: {
      query: BUNDLE_QUERY,
      variables() {
        return {
          limit: this.limit,
        };
      },
      subscribeToMore: {
        document: BUNDLE_SUBSCRIPTION,
        variables() {
          return {
            limit: this.limit,
          };
        },
        updateQuery(previousResult, { subscriptionData }) {
          this.bundle = subscriptionData.data.bundle;
        },
      },
    },
  },


};
</script>
<style lang="scss">
  .bundle-table {
    width: 100%;

    &__timestamp {
      width: 200px;
    }

    &__line {
      &:nth-child(2n) {
        background-color: #eee;
      }
    }

    &__data-values {
      th, td {
        text-align: left;
      }
    }
  }

  .list-move {
    transition: transform 300ms;
  }
</style>
