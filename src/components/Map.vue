<template>
  <div class="map">
    <l-map
      class="map__map"
      :bounds="bounds"
      @update:bounds="boundsUpdated"
    >
      <l-tile-layer :url="url" :detectRetina="true"></l-tile-layer>
    </l-map>
  </div>
</template>

<script>

import { LMap, LTileLayer, LMarker } from 'vue2-leaflet';
import 'leaflet/dist/leaflet.css';

export default {
  components: {
    LMap,
    LTileLayer,
    LMarker,
  },
  data() {
    return {
      url: 'http://{s}.tile.osm.org/{z}/{x}/{y}.png',
      bounds: [
        [52.116731083397006, 5.330257415771485],
        [52.19319269443596, 5.4567718505859375],
      ],
    };
  },
  methods: {
    boundsUpdated(bounds) {
      this.bounds = bounds;
    },
  },
};
</script>
<style lang="scss">
  .map {
    position: relative;

    &__info {
      position: absolute;
      top: 100px;
      left: 100px;
      z-index: 1000;
    }
  }
</style>
