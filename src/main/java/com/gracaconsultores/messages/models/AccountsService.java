package com.gracaconsultores.pic.hazelcast.demo.services;


import com.gracaconsultores.pic.hazelcast.demo.model.ResponseModels;
import com.gracaconsultores.pic.hazelcast.demo.model.output.WPM80A;
import com.gracaconsultores.pic.integrator.reactive.OutputMessage;
import com.gracaconsultores.pic.integrator.reactive.TransformService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

@Slf4j
@Service
public class AccountsService implements TransformService {

    @Override
    public Mono<Object> getOutPut(OutputMessage outPutMessage) {
        try {
            if (outPutMessage != null) {
                log.info("outPutMessage : " + outPutMessage);
                if (outPutMessage.getResult() != null) {
                    Collection<HashMap<String, Object>> maps = outPutMessage.getResult().get("WPM80A");
                    List<WPM80A> wpm80AList = new ArrayList<>();
                    if (maps != null && !maps.isEmpty()) {
                        //HashMap<String,Object> map= (HashMap<String,Object>) maps.toArray()[0];
                        for (HashMap<String, Object> map : maps) {
                            WPM80A wpm80A = new WPM80A();
                            wpm80A.setNumeroCliente(map.get("PENUMPE"));
                            wpm80A.setNumeroContrato((String) map.get("PENUMCO"));
                            wpm80A.setOficinaDeCcc((String) map.get("PECODOF"));
                            wpm80A.setEntidadDeCcc((String) map.get("PECODEN"));
                            wpm80A.setCalidadParticipe(map.get("PECALPA"));
                            wpm80A.setOrderParticipe(map.get("PEORDPA"));
                            wpm80A.setCodigoProducto(map.get("PECODPR"));
                            wpm80A.setCodigoSubproducto(map.get("PESUBPR"));
                            wpm80A.setFechaDeBaja(map.get("PEFECAB"));
                            wpm80A.setEstadoRelacion(map.get("PEESTRE"));
                            wpm80A.setResponsabilidad(map.get("PERESIN"));
                            wpm80A.setMarcaRetener(map.get("PEMARPA"));
                            wpm80A.setMotivoDeLaBaja(map.get("PEMOTBA"));
                            wpm80A.setTimestamp(map.get("PESTAMP"));
                            wpm80A.setCodigoDeMoneda(map.get("PECODMO"));
                            //CuentaContrato=  entidad-de-ccc + oficina-de-ccc + numero-contrato
                            wpm80A.setCuentaContrato(wpm80A.getEntidadDeCcc() + wpm80A.getOficinaDeCcc() + wpm80A.getNumeroContrato());
                            wpm80AList.add(wpm80A);
                        }
                    }
                    return Mono.just(new ResponseModels(Integer.valueOf(outPutMessage.getCode()), outPutMessage.getDescription().replace("\u0000", ""), wpm80AList, 200));
                } else {
                    return Mono.just(new ResponseModels(1001, outPutMessage.getDescription().replace("\u0000", ""), null, 200));
                }
            } else {
                log.info("error exception :{}, detalle: {} ", "Transaccion no procesada");
                return Mono.just(new ResponseModels(1010, "Transaccion no procesada", null, 200));
            }
        } catch (Exception e) {
            log.info("error exception : " + outPutMessage.getDescription());
            return Mono.just(new ResponseModels(1020, outPutMessage.getDescription().replace("\u0000", ""), null, 200));
        }
    }
}
