import threading
import ngrok
import asyncio
__VER__ = '0.0.0.0'


class _NgrokMixinPlugin(object):
  class NgrokCT:
    NG_TOKEN = 'EE_NGROK_AUTH_TOKEN'
    # HTTP_GET = 'get'
    # HTTP_PUT = 'put'
    # HTTP_POST = 'post'

  """
  A plugin which exposes all of its methods marked with @endpoint through
  fastapi as http endpoints, and further tunnels traffic to this interface
  via ngrok.

  The @endpoint methods can be triggered via http requests on the web server
  and will be processed as part of the business plugin loop.
  """

  @property
  def app_url(self):
    return None if self.ngrok_listener is None else self.ngrok_listener.url()

  def get_setup_commands(self):
    try:
      super_setup_commands = super(_NgrokMixinPlugin, self).get_setup_commands()
    except AttributeError:
      super_setup_commands = []
    if self.cfg_ngrok_use_api:
      # In this case the authentification will be made through the api in the actual code,
      # instead of the command line.
      return super_setup_commands
    # endif ngrok api used

    if self.cfg_ngrok_enabled:
      return [self.__get_ngrok_auth_command()] + super_setup_commands
    else:
      return super_setup_commands

  def report_missing_authtoken(self):
    msg = "Ngrok token not found. Please set the environment variable `EE_NGROK_AUTH_TOKEN`"
    # Maybe have notif_code in the future
    self.P(msg, color='r')
    self._create_notification(
      msg=msg,
    )
    return

  def maybe_init_ngrok(self):
    if self.cfg_ngrok_use_api and not self.ngrok_initiated:
      self.ngrok_initiated = True
      ng_token = self.__get_ng_token()
      if ng_token is None:
        self.report_missing_authtoken()
      else:
        ngrok.set_auth_token(ng_token)
        self.P(f"Ngrok initiated for {self.unique_identification}.")
      # endif ng_token present
    # endif ngrok api used
    return

  def get_ngrok_tunnel_kwargs(self):
    """
    TODO:
      - in the case of container in container we will need to add `addr` parameter to the
        tunnel_kwargs as we might have a local network within the Edge Node.
    """
    # Make the ngrok tunnel kwargs
    tunnel_kwargs = {}
    valid = True
    if self.cfg_ngrok_edge_label is not None:
      # In case of using edge label, the domain is not needed and the protocol is "labeled".
      tunnel_kwargs['labels'] = f'edge:{self.cfg_ngrok_edge_label}'
      tunnel_kwargs['proto'] = "labeled"
    # endif edge label
    elif self.cfg_ngrok_domain is not None:
      # In case of using domain, the domain is needed and the protocol is "http"(the default value).
      tunnel_kwargs['domain'] = self.cfg_ngrok_domain
    # endif domain
    # Specify the address and the authtoken
    tunnel_kwargs['addr'] = self.port
    ng_token = self.__get_ng_token()
    if ng_token is None:
      valid = False
      self.report_missing_authtoken()
    tunnel_kwargs['authtoken'] = ng_token
    return tunnel_kwargs, valid


  async def __maybe_async_stop_ngrok(self):
    try:
      self.P(f"Ngrok stopping...")
      self.ngrok_listener.close()
      self.ngrok_started = False
      self.P(f"Ngrok stopped.")
    except Exception as exc:
      self.P(f"Error stopping ngrok: {exc}", color='r')
    return

  
  def maybe_stop_ngrok(self):
    if self.ngrok_started and self.ngrok_listener is not None:
      self.P(f"Closing Ngrok listener...")

      threading.Thread(target=lambda: asyncio.run(self.__maybe_async_stop_ngrok()), daemon=True).start()
    return


  def maybe_start_ngrok(self):
    """
    
    TODO: 
      - if no edge/domain is specified, the api should generate a url and return it while
        persisting the url in the instance local cache for future use. When the instance is 
        restarted, the same url should be used.
    """
    # Maybe make this asynchronous?
    if self.cfg_ngrok_use_api and not self.ngrok_started:
      self.P(f"Ngrok starting for {self.unique_identification}...")
      tunnel_kwargs, valid = self.get_ngrok_tunnel_kwargs()
      if valid:
        if self.cfg_debug_web_app:
          self.P(f'Ngrok tunnel kwargs: {tunnel_kwargs}')
        self.ngrok_listener = ngrok.forward(**tunnel_kwargs)
        if self.app_url is not None:
          self.P(f"Ngrok started on URL `{self.app_url}` ({self._signature}).")
        else:
          edge = tunnel_kwargs.get('labels')
          domain = tunnel_kwargs.get('domain')
          str_tunnel = f"edge `{edge}`" if edge is not None else f"domain `{domain}`"
          self.P(f"Ngrok started on {str_tunnel} ({self._signature}).")
        # endif app_url
        self.ngrok_started = True
      # endif valid tunnel kwargs
    # endif ngrok api used and not started
    return

  def get_start_commands(self):
    try:
      super_start_commands = super(_NgrokMixinPlugin, self).get_start_commands()
    except AttributeError:
      super_start_commands = []
    if self.cfg_ngrok_use_api:
      # In case of using the ngrok api, the tunnel will be started through the api
      return super_start_commands
    # endif ngrok api used

    if self.cfg_ngrok_enabled:
      return [self.__get_ngrok_start_command()] + super_start_commands
    else:
      return super_start_commands

  def __get_ng_token(self):
    # TODO: At the moment multiple user auth tokens will not work on the same node
    #  if `NGROK_USE_API` is set to False.
    #  For the same node to use more than one auth token it needs to do so in separated
    #  processes. For the moment this is done only if the ngrok is started through
    #  CLI commands.
    configured_ng_token = self.cfg_ngrok_auth_token
    environment_ng_token = self.os_environ.get(_NgrokMixinPlugin.NgrokCT.NG_TOKEN, None)
    if self.cfg_debug_web_app:
      self.P(f"Configured token: {configured_ng_token}, Environment token: {environment_ng_token}")
    return configured_ng_token if configured_ng_token is not None else environment_ng_token

  def __get_ngrok_auth_command(self):
    return f"ngrok authtoken {self.__get_ng_token()}"

  def __get_ngrok_start_command(self):
    if self.cfg_ngrok_edge_label is not None:
      return f"ngrok tunnel {self.port} --label edge={self.cfg_ngrok_edge_label}"
    elif self.cfg_ngrok_domain is not None:
      return f"ngrok http {self.port} --domain={self.cfg_ngrok_domain}"
    else:
      raise RuntimeError("No domain/edge specified. Please check your configuration.")
    # endif
