<template>
  <div id="app">
    <router-view/>
  </div>
</template>

<style lang="scss">
  body, html {
    display: grid;
    margin: 0;
    min-height: 100vh;
  }
  #app {
    font-family: sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    text-align: center;
    color: #333;

    display: grid;
  }
</style>
