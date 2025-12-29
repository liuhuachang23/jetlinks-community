package org.jetlinks.community.simulator.core.listener;

import lombok.SneakyThrows;
import org.jetlinks.community.simulator.core.Session;
import org.jetlinks.community.simulator.core.Simulator;
import org.jetlinks.community.simulator.core.SimulatorListener;

import javax.script.*;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * 使用jsr223脚本引擎来执行监听器逻辑
 * <p>
 * 内置变量:
 * <ul>
 *     <li>
 *         listener : 监听器实例
 *     </li>
 *     <li>
 *         simulator: 模拟器实例
 *     </li>
 * </ul>
 *
 * 通过在脚本中调用内置变量的方法来进行逻辑处理，比如:
 * <pre lang="javascript">
 *   listener.onBefore(function(session){
 *
 *       //创建连接前处理逻辑
 *
 *   });
 *
 *   listener.onAfter(function(session){
 *      //创建连接后处理逻辑
 *   });
 *
 *   var _simulator = simulator;
 *   _simulator.doOnComplete(function(){
 *       //模拟器启动完成
 *
 *
 *   })
 * </pre>
 * @author zhouhao
 * @since 1.6
 */
public class JSR223Listener implements SimulatorListener {

    private final String id;

    final List<Consumer<Session>> beforeListener = new CopyOnWriteArrayList<>();
    final List<Consumer<Session>> afterListener = new CopyOnWriteArrayList<>();
    private final CompiledScript compiledScript;
    private final ScriptEngine engine;

    @SneakyThrows
    public JSR223Listener(String id, String lang, String script) {
        this.id = id;
        ScriptEngineManager manager = new ScriptEngineManager();
        this.engine = manager.getEngineByName(lang);
        if (engine == null) {
            throw new UnsupportedOperationException("不支持的脚本语言:" + lang);
        }
        if (engine instanceof Compilable) {
            this.compiledScript = ((Compilable) engine).compile(script);
        } else {
            throw new UnsupportedOperationException("脚本引擎不支持编译: " + lang);
        }
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getType() {
        return "jsr223";
    }

    @Override
    public boolean supported(Simulator simulator) {
        return true;
    }

    @Override
    @SneakyThrows
    public void init(Simulator simulator) {
        Bindings bindings = new SimpleBindings();
        bindings.put("listener", this);
        bindings.put("simulator", simulator);

        compiledScript.eval(bindings);
    }

    public void onBefore(Consumer<Session> session) {
        this.beforeListener.add(session);
    }

    public void onAfter(Consumer<Session> session) {
        this.afterListener.add(session);
    }

    @Override
    public void before(Session session) {
        for (Consumer<Session> listener : beforeListener) {
            listener.accept(session);
        }
    }

    @Override
    public void after(Session session) {
        for (Consumer<Session> listener : afterListener) {
            listener.accept(session);
        }
    }

    @Override
    public void shutdown() {
        beforeListener.clear();
        afterListener.clear();
    }
}
