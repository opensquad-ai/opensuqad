# -*- coding: utf-8 -*-
"""
OpenSquad Plugin Manager

Discovers, loads, and manages all plugins in the plugins/ directory.
All plugins must use the new-style decorator API (opensquad.plugin_api).

Integrates with boot.py to register plugin-provided tools into agent ToolRegistry.
Provides hook chain execution for runner.py lifecycle hooks.
"""
import importlib
import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger("plugins.manager")


class PluginManager:
    """
    Central manager for all OpenSquad plugins.

    Plugin style:
    - Plugin class decorated with @register(...)
    - Methods decorated with @tool, @hook.on_xxx, @on_event
    - plugin.json is auto-generated from decorators

    Usage in boot.py:
        pm = PluginManager()
        pm.discover_and_load()
        pm.register_tools_to_agent(registry, agent_id, agent_tool_names)

    Usage in runner.py:
        ctx = await pm.run_hook("on_message_received", context)
    """

    def __init__(self, plugins_dir: str = None, agent_id: str = ""):
        """
        Args:
            plugins_dir: absolute path to the plugins/ directory.
                         Defaults to the directory containing this file.
            agent_id: ID of the agent this plugin manager serves.
        """
        if plugins_dir is None:
            plugins_dir = os.path.dirname(os.path.abspath(__file__))
        self.plugins_dir = plugins_dir
        self.agent_id = agent_id

        # {plugin_name: {
        #     "plugin": instance,
        #     "metadata": dict,
        #     "dir": str,
        #     "hook_map": {hook_name: [bound_method, ...]},
        #     "tool_wrappers": [ToolModuleWrapper, ...],
        # }}
        self._plugins: Dict[str, Dict[str, Any]] = {}

        # Cached hook chain: {hook_name: [(priority, plugin_name, bound_method), ...]}
        self._hook_chain_cache: Optional[Dict[str, List]] = None

        # Hot-reload: track EventBus subscriptions per plugin for clean unload
        # {plugin_name: [(event_type, callback), ...]}
        self._event_subscriptions: Dict[str, List] = {}

        # Hot-reload: last known timestamp from .reload_ts file
        self._last_reload_ts: float = 0.0

    def discover_and_load(self) -> List[str]:
        """
        Scan plugins/ directory for plugin directories, load each plugin.

        Discovery:
        1. Look for plugin.py in each subdirectory
        2. Find class with __plugin_meta__ (@register decorator)
        3. Use decorator metadata, auto-generate plugin.json

        Returns:
            List of loaded plugin names.
        """
        loaded = []
        if not os.path.isdir(self.plugins_dir):
            logger.warning(f"[PluginManager] Plugins directory not found: {self.plugins_dir}")
            return loaded

        for entry in sorted(os.listdir(self.plugins_dir)):
            plugin_dir = os.path.join(self.plugins_dir, entry)
            if not os.path.isdir(plugin_dir):
                continue

            plugin_py = os.path.join(plugin_dir, "plugin.py")
            if not os.path.isfile(plugin_py):
                continue

            try:
                name = self._load_plugin(plugin_dir, entry)
                if name:
                    loaded.append(name)
            except Exception as e:
                logger.error(f"[PluginManager] Failed to load plugin from {entry}: {e}", exc_info=True)

        self._hook_chain_cache = None
        logger.info(f"[PluginManager] Loaded {len(loaded)} plugins: {loaded}")
        return loaded

    def _load_plugin(self, plugin_dir: str, dir_name: str) -> Optional[str]:
        """
        Import plugin.py and load the plugin class (must have __plugin_meta__).
        """
        module_path = f"plugins.{dir_name}.plugin"
        try:
            plugin_module = importlib.import_module(module_path)
        except ImportError as e:
            logger.error(f"[PluginManager] Cannot import {module_path}: {e}")
            return None

        plugin_class = None
        for attr_name in dir(plugin_module):
            attr = getattr(plugin_module, attr_name)
            if isinstance(attr, type) and hasattr(attr, "__plugin_meta__"):
                plugin_class = attr
                break

        if plugin_class is None:
            logger.error(f"[PluginManager] No @register plugin class found in {module_path}")
            return None

        return self._load_new_style(plugin_class, plugin_dir, dir_name)

    # ------------------------------------------------------------------
    # Plugin loading
    # ------------------------------------------------------------------

    def _load_new_style(self, plugin_class, plugin_dir: str, dir_name: str) -> Optional[str]:
        """
        Load a plugin decorated with @register.

        Steps:
        1. Extract metadata from __plugin_meta__
        2. Check enabled status from existing plugin.json (if any)
        3. Build Context object
        4. Instantiate plugin with Context
        5. Scan @tool, @hook, @on_event decorators
        6. Auto-generate/update plugin.json
        7. Call plugin.on_load()
        """
        from opensquad.plugin_api import (
            Context, ToolModuleWrapper,
            get_plugin_meta, get_tool_methods, get_hook_methods,
            get_event_methods, generate_plugin_json,
        )

        meta = get_plugin_meta(plugin_class)
        if not meta:
            return None

        name = meta["name"]
        plugin_type = meta["type"]

        # Check enabled status from existing plugin.json
        manifest_path = os.path.join(plugin_dir, "plugin.json")
        if os.path.isfile(manifest_path):
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                if not existing.get("enabled", True):
                    logger.info(f"[PluginManager] Plugin '{name}' is disabled, skipping.")
                    return None
            except Exception:
                pass

        # Build Context
        project_root = os.path.dirname(self.plugins_dir)
        data_dir = os.path.join(project_root, "data", "plugins", name)

        event_bus = None
        try:
            from opensquad.events import bus
            event_bus = bus
        except ImportError:
            pass

        # Resolve config values: schema defaults first, then persisted user values override
        config_schema = meta.get("config_schema", {})
        config_values = {}
        for key, schema in config_schema.items():
            if isinstance(schema, dict) and "default" in schema:
                config_values[key] = schema["default"]

        # Load persisted config saved via UI (data/plugins/{name}/config.json)
        persisted_config_path = os.path.join(project_root, "data", "plugins", name, "config.json")
        if os.path.isfile(persisted_config_path):
            try:
                import json as _json
                with open(persisted_config_path, "r", encoding="utf-8") as _f:
                    persisted = _json.load(_f)
                if isinstance(persisted, dict):
                    config_values.update(persisted)
            except Exception:
                pass

        context = Context(
            agent_id=self.agent_id,
            project_root=project_root,
            event_bus=event_bus,
            config=config_values,
            data_dir=data_dir,
            plugin_dir=plugin_dir,
        )

        # Instantiate plugin
        plugin_instance = plugin_class(context)
        plugin_instance.name = name
        plugin_instance.version = meta.get("version", "1.0.0")
        plugin_instance.plugin_type = plugin_type

        # Scan @tool methods -> build ToolModuleWrappers
        tool_wrappers = []
        tool_methods = get_tool_methods(plugin_instance)
        if tool_methods:
            ns_groups: Dict[str, List] = {}
            for tm in tool_methods:
                ns = tm["meta"]["name"]
                if ns not in ns_groups:
                    ns_groups[ns] = []
                ns_groups[ns].append(tm)

            for ns, methods in ns_groups.items():
                wrapper = ToolModuleWrapper(plugin_instance, namespace=ns)
                for tm in methods:
                    wrapper.add_method(
                        method_name=tm["method_name"],
                        bound_method=tm["bound_method"],
                        doc=tm["bound_method"].__doc__ or "",
                    )
                tool_wrappers.append({
                    "wrapper": wrapper,
                    "namespace": ns,
                    "meta": methods[0]["meta"],
                })

        # Scan @hook methods
        hook_map = get_hook_methods(plugin_instance)

        # Scan @on_event methods -> auto-subscribe to EventBus
        event_methods = get_event_methods(plugin_instance)
        if event_methods and event_bus:
            self._event_subscriptions[name] = []
            for em in event_methods:
                event_bus.subscribe(em["event_type"], em["bound_method"])
                self._event_subscriptions[name].append(
                    (em["event_type"], em["bound_method"])
                )
                logger.info(f"[PluginManager] Plugin '{name}': subscribed to "
                            f"EventBus '{em['event_type']}'")

        # Auto-generate plugin.json
        generated = generate_plugin_json(plugin_class, plugin_instance)

        # If plugin has get_tool_modules() (proxy pattern), merge those tools
        if hasattr(plugin_instance, "get_tool_modules"):
            try:
                proxy_tools = plugin_instance.get_tool_modules()
                existing_names = {t["name"] for t in generated.get("tools", [])}
                for pt in proxy_tools:
                    pt_name = pt.get("name", "")
                    if pt_name and pt_name not in existing_names:
                        generated.setdefault("tools", []).append({
                            "name": pt_name,
                            "module": "proxy",
                            "level": pt.get("level", "extended"),
                            "auto_register": pt.get("auto_register", False),
                            "requires_agent_id": pt.get("requires_agent_id", False),
                        })
            except Exception as e:
                logger.debug(f"[PluginManager] get_tool_modules() failed for '{name}': {e}")

        # Preserve enabled field from existing file
        if os.path.isfile(manifest_path):
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                generated["enabled"] = existing.get("enabled", True)
            except Exception:
                pass

        try:
            os.makedirs(plugin_dir, exist_ok=True)
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(generated, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"[PluginManager] Failed to write plugin.json for '{name}': {e}")

        # Build full metadata
        metadata = generated.copy()
        metadata["_runtime"] = {
            "agent_id": self.agent_id,
            "project_root": project_root,
        }

        # Call on_load()
        plugin_instance.on_load()

        self._plugins[name] = {
            "plugin": plugin_instance,
            "metadata": metadata,
            "dir": plugin_dir,
            "hook_map": hook_map,
            "tool_wrappers": tool_wrappers,
        }

        logger.info(f"[PluginManager] Loaded: {name} v{meta.get('version', '?')} "
                     f"(type={plugin_type}, tools={len(tool_wrappers)}, "
                     f"hooks={list(hook_map.keys())}, events={len(event_methods)})")
        return name

    # ------------------------------------------------------------------
    # Tool registration
    # ------------------------------------------------------------------

    def register_tools_to_agent(self, registry, agent_id: str,
                                agent_tool_names: List[str] = None) -> int:
        """
        Register plugin-provided tools to an agent's ToolRegistry.

        Args:
            registry: ToolRegistry instance
            agent_id: current agent's ID
            agent_tool_names: list of tool names from agent's config.json

        Returns:
            Number of tool modules registered.
        """
        if agent_tool_names is None:
            agent_tool_names = []

        count = 0

        for plugin_name, info in self._plugins.items():
            plugin = info["plugin"]

            # 1) Register @tool decorated methods via ToolModuleWrapper
            for tw in info.get("tool_wrappers", []):
                wrapper = tw["wrapper"]
                namespace = tw["namespace"]
                meta = tw["meta"]
                level = meta.get("level", "extended")
                auto_register = meta.get("auto_register", False)

                if not (auto_register or namespace in agent_tool_names):
                    continue

                registry.register(wrapper, namespace, level=level)
                count += 1
                logger.info(f"[PluginManager] Registered tool '{namespace}' from "
                            f"plugin '{plugin_name}' (level={level})")

            # 2) Also check get_tool_modules() for proxy-pattern tools
            if hasattr(plugin, "get_tool_modules"):
                for desc in plugin.get_tool_modules():
                    tool_name = desc.get("name", "")
                    module = desc.get("module")
                    level = desc.get("level", "extended")
                    auto_register = desc.get("auto_register", False)
                    requires_agent_id = desc.get("requires_agent_id", False)

                    if not (auto_register or tool_name in agent_tool_names):
                        continue
                    if module is None:
                        continue

                    registry.register(module, tool_name, level=level)
                    if requires_agent_id and hasattr(module, "set_agent_id") and agent_id:
                        module.set_agent_id(agent_id)

                    count += 1
                    logger.info(f"[PluginManager] Registered tool '{tool_name}' from "
                                f"plugin '{plugin_name}' (proxy, level={level})")

        return count

    # ------------------------------------------------------------------
    # Hook chain execution
    # ------------------------------------------------------------------

    def _build_hook_chain(self) -> Dict[str, List]:
        """
        Build the hook chain from all loaded plugins.

        Returns:
            {hook_name: [(priority, plugin_name, bound_method), ...]}
            sorted by (-priority, plugin_name) so higher-priority handlers run first,
            with alphabetical plugin name as the tiebreaker.
        """
        chain: Dict[str, List] = {}

        for name in sorted(self._plugins.keys()):
            hook_map = self._plugins[name].get("hook_map", {})
            for hook_name, methods in hook_map.items():
                if hook_name not in chain:
                    chain[hook_name] = []
                for method in methods:
                    priority = 0
                    if hasattr(method, "__hook_meta__"):
                        for entry in method.__hook_meta__:
                            if entry.get("hook_name") == hook_name:
                                priority = entry.get("priority", 0)
                                break
                    chain[hook_name].append((priority, name, method))

        for hook_name in chain:
            chain[hook_name].sort(key=lambda t: (-t[0], t[1]))

        return chain

    async def run_hook(self, hook_name: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a hook across all registered plugins (chain pattern).

        Handlers are sorted by (-priority, plugin_name) for deterministic execution order.
        A handler can stop the chain by setting context['__stop__'] = True.

        Args:
            hook_name: name of the hook (e.g. "on_message_received")
            context: the context dict to pass through the chain

        Returns:
            The final context dict after all hooks have processed it.
        """
        if self._hook_chain_cache is None:
            self._hook_chain_cache = self._build_hook_chain()

        handlers = self._hook_chain_cache.get(hook_name, [])
        if not handlers:
            return context

        for priority, plugin_name, method in handlers:
            try:
                context = await method(context)
            except Exception as e:
                logger.error(f"[PluginManager] Hook '{hook_name}' error in plugin "
                             f"'{plugin_name}': {e}", exc_info=True)
            if context.get("__stop__"):
                logger.info(f"[PluginManager] Hook '{hook_name}' chain stopped by "
                            f"plugin '{plugin_name}' (priority={priority})")
                break

        return context

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def get_plugin(self, name: str) -> Optional[Any]:
        """Get a loaded plugin by name."""
        info = self._plugins.get(name)
        return info["plugin"] if info else None

    def get_all_plugins(self) -> Dict[str, Any]:
        """Return all loaded plugins as {name: plugin_instance}."""
        return {name: info["plugin"] for name, info in self._plugins.items()}

    def get_plugins_by_type(self, plugin_type: str) -> List[Any]:
        """Return all loaded plugins of a specific type."""
        return [
            info["plugin"]
            for info in self._plugins.values()
            if info["metadata"].get("type") == plugin_type
        ]

    def get_plugin_metadata(self, name: str) -> Optional[Dict[str, Any]]:
        """Get the plugin.json metadata for a plugin."""
        info = self._plugins.get(name)
        return info["metadata"] if info else None

    def list_plugins(self) -> List[Dict[str, str]]:
        """Return a summary list of all loaded plugins."""
        result = []
        for name, info in self._plugins.items():
            meta = info["metadata"]
            result.append({
                "name": name,
                "display_name": meta.get("display_name", name),
                "version": meta.get("version", "0.0.0"),
                "type": meta.get("type", ""),
                "description": meta.get("description", ""),
            })
        return result

    # ------------------------------------------------------------------
    # Hot-Reload support
    # ------------------------------------------------------------------

    def unload_plugin(self, name: str, registry=None) -> bool:
        """
        Fully unload a plugin: call on_unload(), unsubscribe EventBus,
        unregister tools from ToolRegistry, remove from _plugins.

        Args:
            name: plugin name to unload
            registry: ToolRegistry instance (needed to unregister tools)

        Returns:
            True if successfully unloaded.
        """
        info = self._plugins.get(name)
        if not info:
            logger.warning(f"[PluginManager] Cannot unload '{name}': not loaded")
            return False

        plugin = info["plugin"]

        # 1) Call on_unload()
        try:
            plugin.on_unload()
        except Exception as e:
            logger.error(f"[PluginManager] on_unload() error for '{name}': {e}")

        # 2) Unsubscribe from EventBus
        if name in self._event_subscriptions:
            try:
                from opensquad.events import bus
                for event_type, callback in self._event_subscriptions[name]:
                    bus.unsubscribe(event_type, callback)
                    logger.debug(f"[PluginManager] Unsubscribed '{name}' from '{event_type}'")
            except Exception as e:
                logger.error(f"[PluginManager] EventBus unsubscribe error for '{name}': {e}")
            del self._event_subscriptions[name]

        # 3) Unregister tools from ToolRegistry
        if registry:
            for tw in info.get("tool_wrappers", []):
                registry.unregister(tw["namespace"])

            if hasattr(plugin, "get_tool_modules"):
                try:
                    for desc in plugin.get_tool_modules():
                        tool_name = desc.get("name", "")
                        if tool_name:
                            registry.unregister(tool_name)
                except Exception:
                    pass

        # 4) Remove from _plugins and invalidate cache
        del self._plugins[name]
        self._hook_chain_cache = None
        logger.info(f"[PluginManager] Unloaded plugin '{name}'")
        return True

    def reload_plugins(self, registry=None, agent_id: str = "",
                       agent_tool_names: List[str] = None) -> Dict[str, str]:
        """
        Compare disk plugin.json enabled state vs in-memory _plugins.
        Unload newly-disabled plugins, load newly-enabled plugins.

        Args:
            registry: ToolRegistry instance
            agent_id: current agent ID
            agent_tool_names: tool names from agent config

        Returns:
            {"loaded": [...], "unloaded": [...]} summary
        """
        if agent_tool_names is None:
            agent_tool_names = []

        result = {"loaded": [], "unloaded": []}

        # Scan all plugin directories on disk
        disk_plugins = {}
        for entry in sorted(os.listdir(self.plugins_dir)):
            plugin_dir = os.path.join(self.plugins_dir, entry)
            if not os.path.isdir(plugin_dir):
                continue
            if not os.path.isfile(os.path.join(plugin_dir, "plugin.py")):
                continue

            manifest_path = os.path.join(plugin_dir, "plugin.json")
            enabled = True
            plugin_name = entry

            if os.path.isfile(manifest_path):
                try:
                    with open(manifest_path, "r", encoding="utf-8") as f:
                        manifest = json.load(f)
                    enabled = manifest.get("enabled", True)
                    plugin_name = manifest.get("name", entry)
                except Exception:
                    pass

            disk_plugins[plugin_name] = {
                "enabled": enabled,
                "dir": plugin_dir,
                "dir_name": entry,
            }

        # Unload plugins that are now disabled on disk but loaded in memory
        for name in list(self._plugins.keys()):
            disk = disk_plugins.get(name)
            if disk and not disk["enabled"]:
                self.unload_plugin(name, registry=registry)
                result["unloaded"].append(name)

        # Load plugins that are now enabled on disk but not loaded in memory
        for name, disk in disk_plugins.items():
            if disk["enabled"] and name not in self._plugins:
                try:
                    loaded_name = self._load_plugin(disk["dir"], disk["dir_name"])
                    if loaded_name and registry and loaded_name in self._plugins:
                        info = self._plugins[loaded_name]
                        plugin = info["plugin"]

                        for tw in info.get("tool_wrappers", []):
                            ns = tw["namespace"]
                            meta = tw["meta"]
                            level = meta.get("level", "extended")
                            if meta.get("auto_register") or ns in agent_tool_names:
                                registry.register(tw["wrapper"], ns, level=level)

                        if hasattr(plugin, "get_tool_modules"):
                            for desc in plugin.get_tool_modules():
                                t_name = desc.get("name", "")
                                module = desc.get("module")
                                level = desc.get("level", "extended")
                                req_aid = desc.get("requires_agent_id", False)
                                if (desc.get("auto_register") or t_name in agent_tool_names) and module:
                                    registry.register(module, t_name, level=level)
                                    if req_aid and hasattr(module, "set_agent_id") and agent_id:
                                        module.set_agent_id(agent_id)

                    if loaded_name:
                        result["loaded"].append(loaded_name)
                except Exception as e:
                    logger.error(f"[PluginManager] Failed to reload plugin '{name}': {e}",
                                 exc_info=True)

        if result["loaded"] or result["unloaded"]:
            self._hook_chain_cache = None
            logger.info(f"[PluginManager] Reload complete: loaded={result['loaded']}, "
                        f"unloaded={result['unloaded']}")

        return result

    def check_reload_needed(self) -> bool:
        """
        Check if the .reload_ts file has been updated since last check.
        Called periodically by AgentRunner.

        Returns:
            True if reload is needed (timestamp changed).
        """
        ts_file = os.path.join(self.plugins_dir, ".reload_ts")
        if not os.path.isfile(ts_file):
            return False

        try:
            mtime = os.path.getmtime(ts_file)
            if mtime > self._last_reload_ts:
                self._last_reload_ts = mtime
                return True
        except Exception:
            pass

        return False
