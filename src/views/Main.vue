<template>
  <div class="main-view">
    <BundleTable/>
  </div>
</template>
<script>
import MeasurementTable from '@/components/MeasurementTable.vue';
import BundleTable from '@/components/BundleTable.vue';
import Map from '../components/Map.vue';

export default {
  components: {
    Map,
    MeasurementTable,
    BundleTable,
  },
};
</script>
<style lang="scss">
  .main-view {
    display: grid;
  }
</style>
