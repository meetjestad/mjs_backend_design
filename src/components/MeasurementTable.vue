<template>
  <table class="measurement-table">
    <thead>
    <tr>
      <th class="measurement-table__timestamp">Timestamp</th>
      <th class="measurement-table__node">Node / ID</th>
      <th class="measurement-table__data">Data</th>
    </tr>
    </thead>
    <transition-group name="list" tag="tbody">
      <tr v-for="m in measurement" :key="m.id" class="measurement-table__line">
        <td>{{ m.timestamp }}</td>
        <td>
          {{ m.node_id }} / {{ m.id }}
        </td>
        <td>
          <table class="measurement-table__data-values">
            <tbody>
            <tr v-for="entry in Object.entries(m.data)" :key="entry[0]">
              <th>{{ entry[0] }}</th>
              <td>{{ entry[1] }}</td>
            </tr>
            </tbody>
          </table>
        </td>
      </tr>
    </transition-group>
  </table>
</template>

<script>

import MEASUREMENT_QUERY from '../graphql/measurement.graphql';
import MEASUREMENT_SUBSCRIPTION from '../graphql/subscription/measurement.graphql';

export default {
  data() {
    return {
      limit: 10,
      measurement: [],
    };
  },

  apollo: {
    measurement: {
      query: MEASUREMENT_QUERY,
      variables() {
        return {
          limit: this.limit,
        };
      },
      subscribeToMore: {
        document: MEASUREMENT_SUBSCRIPTION,
        variables() {
          return {
            limit: this.limit,
          };
        },
        updateQuery(previousResult, { subscriptionData }) {
          this.measurement = subscriptionData.data.measurement;
        },
      },
    },
  },


};
</script>
<style lang="scss">
  .measurement-table {
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
