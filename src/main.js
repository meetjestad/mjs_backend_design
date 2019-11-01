import Vue from 'vue';

import VueApollo from 'vue-apollo';
import { WebSocketLink } from 'apollo-link-ws';
import ApolloClient from 'apollo-client';
import { InMemoryCache } from 'apollo-cache-inmemory';
import store from './store';
import router from './router';
import App from './App.vue';

Vue.config.productionTip = false;


Vue.use(VueApollo);

const wsLink = new WebSocketLink({
  uri: process.env.VUE_APP_GRAPHQL_WS || 'ws://localhost:4000/graphql',
  options: {
    reconnect: true,
    connectionParams: {
      headers: {
        'x-hasura-admin-secret': process.env.VUE_APP_GRAPHQL_TOKEN,
      },
    },
  },
});


const client = new ApolloClient({
  link: wsLink,
  cache: new InMemoryCache(),
});

const apolloProvider = new VueApollo({
  defaultClient: client,
});

new Vue({
  router,
  store,
  apolloProvider,
  render: h => h(App),
}).$mount('#app');
